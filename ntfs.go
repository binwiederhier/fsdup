package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"heckel.io/fsdup/internal"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
)

const (
	chunkSizeMaxBytes     = 32 * 1024 * 1024
	dedupFileSizeMinBytes = 4 * 1024 * 1024

	// NTFS boot sector (absolute aka relative to file system start)
	ntfsMagicOffset             = 3
	ntfsMagic                   = "NTFS    "
	ntfsSectorSizeOffset        = 11
	ntfsSectorSizeLength        = 2
	ntfsSectorsPerClusterOffset = 13
	ntfsSectorsPerClusterLength = 1
	ntfsMftClusterNumberOffset = 48
	ntfsMftClusterNumberLength = 8

	// FILE entry (relative to FILE offset)
	// https://flatcap.org/linux-ntfs/ntfs/concepts/file_record.html
	ntfsEntryMagic           = "FILE"
	ntfsEntryFirstAttrOffset = 20
	ntfsEntryFirstAttrLength = 2

	// Attribute header / footer (relative to attribute offset)
	// https://flatcap.org/linux-ntfs/ntfs/concepts/attribute_header.html
	ntfsAttrTypeLength    = 4
	ntfsAttrLengthOffset  = 4
	ntfsAttrLengthLength  = 4
	ntfsAttrTypeData      = 0x80
	ntfsAttrTypeEndMarker = 0xFFFFFF

	// Attribute 0x80 / $DATA (relative to attribute offset)
	// https://flatcap.org/linux-ntfs/ntfs/attributes/data.html
	ntfsAttrDataResidentOffset = 8
	ntfsAttrDataResidentLength = 1
	ntfsAttrDataRealSizeOffset = 48
	ntfsAttrDataRealSizeLength = 8
	ntfsAttrDataRunsOffset = 32
	ntfsAttrDataRunsLength = 2

	// Data run structure within $DATA attribute
	// https://flatcap.org/linux-ntfs/ntfs/concepts/data_runs.html
	ntfsAttrDataRunsHeaderOffset = 0
	ntfsAttrDataRunsHeaderLength = 1
	ntfsAttrDataRunsHeaderEndMarker = 0
)

type nftsChunker struct {

}

type fixedSizeChunker struct {

}

type entry struct {
	fileSize int64
	runs     []run
}

type run struct {
	sparse bool
	fromOffset   int64
	toOffset     int64
	firstCluster int64 // signed!
	clusterCount int64
	size int64
}

type chunk struct {
	checksum     []byte
	size         int64
	diskSections []*diskSection
}

type diskSection struct {
	from int64
	to int64
	sparse bool
}

type chunkPart struct {
	chunk *chunk
	from int64
	to int64
}

var ErrDataResident = errors.New("data is resident")
var ErrNoDataAttr = errors.New("no data attribute")
var ErrUnexpectedMagic = errors.New("unexpected magic")
var ErrFileTooSmallToIndex = errors.New("file too small to index")

func readRuns(reader io.ReaderAt, clusterSize int64, offset int64) []run {
	runs := make([]run, 0)
	firstCluster := int64(0)

	for {
		header := readIntLE(reader, offset + ntfsAttrDataRunsHeaderOffset, ntfsAttrDataRunsHeaderLength)

		if header == ntfsAttrDataRunsHeaderEndMarker {
			break
		}

		clusterCountLength := header & 0x0F  // right nibble
		clusterCountOffset := offset + 1
		clusterCount := readIntLE(reader, clusterCountOffset, int64(clusterCountLength))

		firstClusterLength := header >> 4 // left nibble
		firstClusterOffset := int64(clusterCountOffset) + int64(clusterCountLength)
		firstCluster += readIntLE(reader, int64(firstClusterOffset), int64(firstClusterLength)) // relative to previous, can be negative, so signed!

		sparse := firstClusterLength == 0

		fromOffset := int64(firstCluster) * int64(clusterSize)
		toOffset := fromOffset + int64(clusterCount) * int64(clusterSize)

		fmt.Printf("data run offset = %d, header = 0x%x, sparse = %t, length length = 0x%x, offset length = 0x%x, " +
			"cluster count = %d, first cluster = %d, from offset = %d, to offset = %d\n",
			offset, header, sparse, clusterCountLength, firstClusterLength, clusterCount, firstCluster,
			fromOffset, toOffset)

		runs = append(runs, run{
			sparse:       sparse,
			firstCluster: firstCluster,
			clusterCount: clusterCount,
			fromOffset:   fromOffset,
			toOffset:     toOffset,
			size:         toOffset - fromOffset,
		})

		offset += firstClusterLength + clusterCountLength + 1
	}

	return runs
}

func readEntry(reader io.ReaderAt, clusterSize int64, offset int64) (*entry, error) {
	err := readAndCompare(reader, offset, []byte(ntfsEntryMagic))
	if err != nil {
		return nil, err
	}

	relativeFirstAttrOffset := readIntLE(reader, offset + ntfsEntryFirstAttrOffset, ntfsEntryFirstAttrLength)

	entry := &entry{}

	firstAttrOffset := offset + relativeFirstAttrOffset
	attrOffset := firstAttrOffset

	for {
		attrType := readIntLE(reader, attrOffset, ntfsAttrTypeLength)
		attrLen := readIntLE(reader, attrOffset + ntfsAttrLengthOffset, ntfsAttrLengthLength)

		if attrType == ntfsAttrTypeData {
			nonResident := readIntLE(reader, attrOffset +ntfsAttrDataResidentOffset, ntfsAttrDataResidentLength)

			if nonResident == 0 {
				return nil, ErrDataResident
			}

			dataRealSize := readIntLE(reader, attrOffset + ntfsAttrDataRealSizeOffset, ntfsAttrDataRealSizeLength)

/*			if fileSize < 2 * 1024 * 1024 {
				return nil, ErrFileTooSmallToIndex
			}*/

			relativeDataRunsOffset := readIntLE(reader, attrOffset + ntfsAttrDataRunsOffset, ntfsAttrDataRunsLength)
			dataRunFirstOffset := attrOffset + int64(relativeDataRunsOffset)

			entry.fileSize = dataRealSize
			entry.runs = readRuns(reader, clusterSize, dataRunFirstOffset)
		} else if attrType == ntfsAttrTypeEndMarker || attrLen == 0 {
			break
		}

		attrOffset += attrLen
	}

	if entry.runs == nil {
		return nil, ErrNoDataAttr
	}

	return entry, nil
}

func readAndCompare(reader io.ReaderAt, offset int64, expected []byte) error {
	actual := make([]byte, len(expected))
	n, err := reader.ReadAt(actual, offset)

	if err != nil {
		return err
	} else if n != len(actual) || bytes.Compare(expected, actual) != 0 {
		return ErrUnexpectedMagic
	}

	return nil
}

/*
 run: mft data run, pointing to chunkParts with data on disk
 `- chunkPart: pointer to one or many chunkParts of a chunk; a run's data can be spread across multiple chunks

 chunk: data blob, containing data from one or many runs
 `- diskSection: pointer to one or many sections on disk, describing the offsets of how a chunk is assembled

 MFT entry runs:
  File:        [              disk1.ntfs                ]
  NTFS Runs:   [   A     |  B |           C             ]        << 3 runs
  Chunks:      [   1   |   2    |   3     |    4   |  5 ]        << chunk (cut at fixed intervals, across runs)
  Chunk Parts: [   a   |b| c  |d|   e     |    f   |  g ]        << chunkParts (cut at run and chunk boundaries)


  run = 0, chunk = 0
  chunk =

  chunkPart:
    update on chunk end and run end

  diskSection:
    update on


    [   |  |  |   ||   |   |   |   |   |   | ]        << fixed size runBuffer (same size as chunk max size)

 Chunks (and chunkParts):



    [ a | b | c  | d | e | f |g|  h   |i|  j ]        << chunkPart

 Result:
  run-to-chunk mapping: needed for the creation of the manifest
  chunk-to-run mapping: needed to be able to assemble/read the chunk (only the FIRST file is necessary)

  run-to-chunk:
   runA.chunkParts = [a = 1:0-100, b = 2:0-20]
   runB.chunkParts = [c = 2:21-80]
   runC.chunkParts = [d = 2:81-100, e = 3:0-100, f = 4:0-100, g = 5:0-100]

  chunk-to-run:
   chunk1.diskSections = [A: 0-80]
   chunk2.diskSections = [A: 81-100, B: 0-100, C: 0-10]
   chunk3.diskSections = [C: 11-50]
   chunk4.diskSections = [C: 51-80]
   chunk5.diskSections = [C: 81-100]

 */
func dedupFile(fs io.ReaderAt, entry *entry, clusterSize int64, chunkmap map[string]*chunk, diskmap map[int64]*chunkPart) {
	remainingToEndOfFile := entry.fileSize

	currentChunk := &chunk{
		diskSections: make([]*diskSection, 0),
	}

	currentChunkChecksum := sha256.New()

	os.Mkdir("index", 0770)
	f, _ := os.OpenFile("index/temp", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)

	for _, run := range entry.runs {
		fmt.Printf("\nprocessing run at cluster %d, offset %d, cluster count = %d, size = %d\n",
			run.firstCluster, run.fromOffset, run.clusterCount, run.size)

		if run.sparse {
			fmt.Printf("- sparse run\n")

			b := make([]byte, clusterSize)

			for i := 0; i < int(run.clusterCount); i++ {
				currentChunkChecksum.Write(b)
				remainingToEndOfFile -= clusterSize
			}
		} else {
			runBuffer := make([]byte, chunkSizeMaxBytes) // buffer cannot be larger than chunkSizeMaxBytes!
			runOffset := run.fromOffset
			runSize := minInt64(remainingToEndOfFile, run.size) // only read to filesize, doesnt always align with clusters!

			remainingToEndOfFile -= runSize
			remainingToEndOfRun := runSize

			for remainingToEndOfRun > 0 {
				remainingToFullChunk := chunkSizeMaxBytes - currentChunk.size
				runBytesMaxToBeRead := minInt64(minInt64(remainingToEndOfRun, remainingToFullChunk), int64(len(runBuffer)))

				fmt.Printf("- reading diskSection at offset %d to max %d bytes (remaining to end of run = %d, remaining to full chunk = %d, run buffer size = %d)\n",
					runOffset, runBytesMaxToBeRead, remainingToEndOfRun, remainingToFullChunk, len(runBuffer))

				runBytesRead, err := fs.ReadAt(runBuffer[:runBytesMaxToBeRead], runOffset)
				if err != nil {
					panic(err)
				}

				// FIXME remove this
				_, err = f.Write(runBuffer[:runBytesRead])
				if err != nil {
					panic(err)
				}
				// FIXME remove end

				// Add run to chunk(s)
				fmt.Printf("  - bytes read = %d, current chunk size = %d, chunk max = %d, fits in chunk = %t\n", runBytesRead, currentChunk.size, chunkSizeMaxBytes)

				diskmap[runOffset] = &chunkPart{
					chunk: currentChunk,
					from: currentChunk.size,
					to: currentChunk.size + int64(runBytesRead),
				}

				currentChunkChecksum.Write(runBuffer[:runBytesRead])

				currentChunk.size += int64(runBytesRead)
				currentChunk.diskSections = append(currentChunk.diskSections, &diskSection{
					sparse: false,
					from:   runOffset,
					to:     runOffset + int64(runBytesRead),
				})

				fmt.Printf("  -> adding %d bytes to chunk, new chunk size is %d\n", runBytesRead, currentChunk.size)

				// Emit full chunk
				if currentChunk.size >= chunkSizeMaxBytes {
					// Calculate chunk checksum, add to global chunkmap
					currentChunk.checksum = currentChunkChecksum.Sum(nil)
					checksumStr := fmt.Sprintf("%x", currentChunk.checksum)

					fmt.Printf("  -> emmitting full chunk %s, size = %d\n", checksumStr, currentChunk.size)

					if _, ok := chunkmap[checksumStr]; !ok {
						chunkmap[checksumStr] = currentChunk
					}

					// New chunk
					currentChunkChecksum.Reset()
					currentChunk = &chunk{
						diskSections: make([]*diskSection, 0),
					}

					f.Close()
					os.Rename("index/temp", "index/" + checksumStr)
					f, _ = os.OpenFile("index/temp", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
				}

				remainingToEndOfRun -= int64(runBytesRead)
				runOffset += int64(runBytesRead)
			}
		}
	}

	// Finish last chunk
	if currentChunk.size > 0 {
		// Calculate chunk checksum, add to global chunkmap
		currentChunk.checksum = currentChunkChecksum.Sum(nil)
		checksumStr := fmt.Sprintf("%x", currentChunk.checksum)

		fmt.Printf("  -> emmitting LAST chunk = %s, size %d\n\n", checksumStr, currentChunk.size)

		if _, ok := chunkmap[checksumStr]; !ok {
			chunkmap[checksumStr] = currentChunk
		}

		f.Close()
		os.Rename("index/temp", "index/" + checksumStr)
	}

}

func parseNTFS(filename string) {
	fs, err := os.Open(filename)
	if err != nil {
		log.Fatalln("Cannot open file:", err)
	}

	defer fs.Close()

	stat, err := fs.Stat()
	if err != nil {
		panic(err)
	}

	err = readAndCompare(fs, ntfsMagicOffset, []byte(ntfsMagic))
	if err != nil {
		panic(err)
	}

	sectorSize := readIntLE(fs, ntfsSectorSizeOffset, ntfsSectorSizeLength)
	sectorsPerCluster := readIntLE(fs, ntfsSectorsPerClusterOffset, ntfsSectorsPerClusterLength)
	clusterSize := sectorSize * sectorsPerCluster
	mftClusterNumber := readIntLE(fs, ntfsMftClusterNumberOffset, ntfsMftClusterNumberLength)
	mftOffset := mftClusterNumber * clusterSize

	fmt.Printf("sector size = %d, sectors per cluster = %d, cluster size = %d, mft cluster number = %d, mft offset = %d\n",
		sectorSize, sectorsPerCluster, clusterSize, mftClusterNumber, mftOffset)

	mft, err := readEntry(fs, clusterSize, mftOffset)
	if err != nil {
		panic(err)
	}

	fmt.Printf("\n\nMFT: %#v\n\n", mft)

	chunkmap := make(map[string]*chunk, 0)
	diskmap := make(map[int64]*chunkPart, 0)

	for _, run := range mft.runs {
		fmt.Printf("\n\nprocessing run: %#v\n\n", run)
		entries := int(run.clusterCount * sectorsPerCluster)
		firstSector := run.firstCluster * sectorsPerCluster

		for i := 1; i < entries; i++ { // Skip entry 0 ($MFT itself!)
			sector := firstSector + int64(i)
			sectorOffset := sector * sectorSize
			// TODO we should look at entry.allocSize and jump ahead. This will cut the reads in half!

			fmt.Printf("reading entry at sector %d / offset %d:\n", sector, sectorOffset)

			entry, err := readEntry(fs, clusterSize, sectorOffset)
			if err == ErrDataResident || err == ErrNoDataAttr || err == ErrUnexpectedMagic {
				fmt.Printf("skipping FILE: %s\n\n", err.Error())
				continue
			} else if err != nil {
				panic(err)
			}

			if entry.fileSize < dedupFileSizeMinBytes {
				fmt.Printf("skipping FILE: too small\n\n")
				continue
			}

			fmt.Printf("\n\nFILE %#v\n", entry)
			dedupFile(fs, entry, clusterSize, chunkmap, diskmap)
		}
	}

	fmt.Println("chunk map:")
	for _, chunk := range chunkmap {
		fmt.Printf("- chunk %x\n", chunk.checksum)
		for _, diskSection := range chunk.diskSections {
			size := diskSection.to - diskSection.from
			fmt.Printf("  - disk section %d - %d, size %d\n", diskSection.from, diskSection.to, size)
		}
	}

	fmt.Println("\ndisk map:")

	for offset, chunkPart := range diskmap {
		size := chunkPart.to - chunkPart.from
		sector := offset / sectorSize
		cluster := sector / sectorsPerCluster

		fmt.Printf("- offset %d - %d (sector %d, cluster %d), chunk %x, offset %d - %d, size %d\n",
			offset, offset + size, sector, cluster, chunkPart.chunk.checksum, chunkPart.from, chunkPart.to, size)
	}


	println()
	println("manifest:")

	// Create manifest
	breakpoints := make([]int64, 0, len(diskmap))
	for breakpoint, _ := range diskmap {
		breakpoints = append(breakpoints, breakpoint)
	}

	sort.Slice(breakpoints, func(i, j int) bool {
		return breakpoints[i] < breakpoints[j]
	})

	for _, b := range breakpoints {
		println(b)
	}


	manifest := &internal.ManifestV1{}
	manifest.Size = stat.Size()
	manifest.Slices = make([]*internal.Slice, 0)

	fileSize := stat.Size()

	currentOffset := int64(0)
	breakpointIndex := 0
	breakpoint := int64(0)

	chunkBuffer := make([]byte, chunkSizeMaxBytes)
	/*currentChunk := &chunk{
		diskSections: make([]*diskSection, 0),
	}
*/
	currentChunkChecksum := sha256.New()

	os.Mkdir("index", 0770)
//	f, _ := os.OpenFile("index/temp", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)

	for currentOffset < fileSize {
		hasNextBreakpoint := breakpointIndex < len(breakpoints)

		if hasNextBreakpoint {
			breakpoint = breakpoints[breakpointIndex]
			bytesToBreakpoint := breakpoint - currentOffset
			//fmt.Printf("i = %d, len = %d, breakpoint = %d, currentOffset = %d, bytes = %d\n",
			//	breakpointIndex, len(breakpoints), breakpoint, currentOffset, bytesToBreakpoint)

			if bytesToBreakpoint > chunkSizeMaxBytes {
				// FIXME emit chunk

				chunkEndOffset := minInt64(currentOffset + chunkSizeMaxBytes, fileSize)
				chunkSize := chunkEndOffset - currentOffset

				// EMIT CHUNK BEGIN
				bytesRead, err := fs.ReadAt(chunkBuffer, currentOffset)
				if err != nil {
					panic(err)
				}
				if bytesRead != chunkSizeMaxBytes {
					panic(fmt.Errorf("cannot read all bytes from disk, %d read\n", bytesRead))
				}

				currentChunkChecksum.Reset()
				currentChunkChecksum.Write(chunkBuffer[:bytesRead])
				checksum := currentChunkChecksum.Sum(nil)

				if err := writeChunkFile(checksum, chunkBuffer[:bytesRead]); err != nil {
					panic(err)
				}

				// EMIT CHUNK END

				fmt.Printf("offset %d - %d, NEW chunk %x, size %d\n", currentOffset, chunkEndOffset, checksum, chunkSize)

				manifest.Slices = append(manifest.Slices, &internal.Slice{
					Checksum: checksum,
					Offset: 0,
					Length: chunkSizeMaxBytes,
				})

				currentOffset = chunkEndOffset
			} else {
				if bytesToBreakpoint > 0 {
					// FIXME this should just buffer the current chunk and not emit is right away. It should FILL UP a chunk later!

					// EMIT CHUNK BEGIN
					bytesRead, err := fs.ReadAt(chunkBuffer[:bytesToBreakpoint], currentOffset)
					if err != nil {
						panic(err)
					}
					if int64(bytesRead) != bytesToBreakpoint {
						panic(fmt.Errorf("cannot read all bytes from disk, %d read\n", bytesRead))
					}

					currentChunkChecksum.Reset()
					currentChunkChecksum.Write(chunkBuffer[:bytesRead])
					checksum := currentChunkChecksum.Sum(nil)

					if err := writeChunkFile(checksum, chunkBuffer[:bytesRead]); err != nil {
						panic(err)
					}

					// EMIT CHUNK END

					manifest.Slices = append(manifest.Slices, &internal.Slice{
						Checksum: checksum,
						Offset: 0,
						Length: int64(bytesRead),
					})

					fmt.Printf("offset %d - %d, NEW2 chunk %x, size %d\n", currentOffset, currentOffset + bytesToBreakpoint, checksum, bytesToBreakpoint)
					currentOffset += bytesToBreakpoint
				}

				part := diskmap[breakpoint]
				chunk := part.chunk
				partSize := part.to - part.from

				fmt.Printf("offset %d - %d, existing chunk %x, offset %d - %d, part size %d\n",
					currentOffset, currentOffset + partSize, chunk.checksum, part.from, part.to, partSize)

				manifest.Slices = append(manifest.Slices, &internal.Slice{
					Checksum: chunk.checksum,
					Offset: part.from,
					Length: partSize,
				})

				currentOffset += partSize
				breakpointIndex++
			}
		} else {
			// FIXME emit chunk

			chunkEndOffset := minInt64(currentOffset + chunkSizeMaxBytes, fileSize)
			chunkSize := chunkEndOffset - currentOffset

			// EMIT CHUNK BEGIN
			bytesRead, err := fs.ReadAt(chunkBuffer[:chunkSize], currentOffset)
			if err != nil {
				panic(err)
			}
			if int64(bytesRead) != chunkSize {
				panic(fmt.Errorf("cannot read bytes from disk, %d read\n", bytesRead))
			}

			currentChunkChecksum.Reset()
			currentChunkChecksum.Write(chunkBuffer[:bytesRead])
			checksum := currentChunkChecksum.Sum(nil)

			if err := writeChunkFile(checksum, chunkBuffer[:bytesRead]); err != nil {
				panic(err)
			}

			// EMIT CHUNK END

			fmt.Printf("offset %d - %d, NEW3 chunk %x, size %d\n", currentOffset, chunkEndOffset, checksum, chunkSize)
			manifest.Slices = append(manifest.Slices, &internal.Slice{
				Checksum: checksum,
				Offset: 0,
				Length: int64(bytesRead),
			})

			currentOffset = chunkEndOffset
		}
	}

	println()
	println("manifest (protobuf):")

	offset := int64(0)

	for _, slice := range manifest.Slices {
		fmt.Printf("%010d - chunk %x - offset %10d - %10d, size %d\n",
			offset, slice.Checksum, slice.Offset, slice.Offset + slice.Length, slice.Length)

		offset += slice.Length
	}

	out, err := proto.Marshal(manifest)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	}
	if err := ioutil.WriteFile(filename + ".manifest", out, 0644); err != nil {
		log.Fatalln("Failed to write address book:", err)
	}
}

func writeChunkFile(checksum []byte, data []byte) error {
	chunkFile := fmt.Sprintf("index/%x", checksum)

	if _, err := os.Stat(chunkFile); err != nil {
		err = ioutil.WriteFile(chunkFile, data, 0666)
		if err != nil {
			return err
		}
	}

	return nil
}

type chunk2 struct {
	data []byte
	checksum     []byte
	size         int64
	diskSections []*diskSection
}

func (c *chunk2) Write() {

}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

