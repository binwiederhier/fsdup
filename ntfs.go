package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"heckel.io/fsdup/internal"
	"io"
	"io/ioutil"
	"os"
	"sort"
)

const (
	chunkSizeMaxBytes     = 32 * 1024 * 1024
	dedupFileSizeMinBytes = 4 * 1024 * 1024

	// NTFS boot sector (absolute aka relative to file system start)
	ntfsBootRecordSize              = 512
	ntfsBootMagicOffset             = 3
	ntfsBootMagic                   = "NTFS    "
	ntfsBootSectorSizeOffset        = 11
	ntfsBootSectorSizeLength        = 2
	ntfsBootSectorsPerClusterOffset = 13
	ntfsBootSectorsPerClusterLength = 1
	ntfsBootMftClusterNumberOffset  = 48
	ntfsBootMftClusterNumberLength  = 8

	// FILE entry (relative to FILE offset)
	// https://flatcap.org/linux-ntfs/ntfs/concepts/file_record.html
	ntfsEntryMagic                      = "FILE"
	ntfsEntryUpdateSequenceOffsetOffset = 4
	ntfsEntryUpdateSequenceOffsetLength = 2
	ntfsEntryUpdateSequenceSizeOffset   = 6
	ntfsEntryUpdateSequenceSizeLength   = 2
	ntfsEntryUpdateSequenceNumberLength = 2
	ntfsEntryAllocatedSizeOffset        = 28
	ntfsEntryAllocatedSizeLength        = 4
	ntfsEntryFirstAttrOffset            = 20
	ntfsEntryFirstAttrLength            = 2

	// Attribute header / footer (relative to attribute offset)
	// https://flatcap.org/linux-ntfs/ntfs/concepts/attribute_header.html
	ntfsAttrTypeOffset    = 0
	ntfsAttrTypeLength    = 4
	ntfsAttrLengthOffset  = 4
	ntfsAttrLengthLength  = 4
	ntfsAttrTypeData      = 0x80
	ntfsAttrTypeEndMarker = -1 // 0xFFFFFFFF

	// Attribute 0x80 / $DATA (relative to attribute offset)
	// https://flatcap.org/linux-ntfs/ntfs/attributes/data.html
	ntfsAttrDataResidentOffset = 8
	ntfsAttrDataResidentLength = 1
	ntfsAttrDataRealSizeOffset = 48
	ntfsAttrDataRealSizeLength = 4
	ntfsAttrDataRunsOffset = 32
	ntfsAttrDataRunsLength = 2

	// Data run structure within $DATA attribute
	// https://flatcap.org/linux-ntfs/ntfs/concepts/data_runs.html
	ntfsAttrDataRunsHeaderOffset = 0
	ntfsAttrDataRunsHeaderLength = 1
	ntfsAttrDataRunsHeaderEndMarker = 0
)

type ntfsDeduper struct {
	reader io.ReaderAt
	size int64
	sectorSize int64
	sectorsPerCluster int64
	clusterSize int64
}

type fixedSizeChunker struct {

}

type entry struct {
	allocatedSize int64
	dataSize      int64
	runs          []run
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


func NewNtfsDeduper(reader io.ReaderAt, size int64) *ntfsDeduper {
	return &ntfsDeduper{
		reader: reader,
		size: size,
	}
}

func (self *ntfsDeduper) readEntry(offset int64) (*entry, error) {
	err := readAndCompare(self.reader, offset, []byte(ntfsEntryMagic))
	if err != nil {
		return nil, err
	}

	// Read full entry into buffer (we're guessing size 1024 here)
	buffer := make([]byte, 1024)
	_, err = self.reader.ReadAt(buffer, offset)
	if err != nil {
		return nil, err
	}

	// Read entry length, and re-read the buffer if it differs
	allocatedSize := parseUintLE(buffer, ntfsEntryAllocatedSizeOffset, ntfsEntryAllocatedSizeLength)
	if int(allocatedSize) > len(buffer) {
		buffer = make([]byte, allocatedSize)
		_, err = self.reader.ReadAt(buffer, offset)
		if err != nil {
			return nil, err
		}
	}

	// Apply fix-up
	// see https://flatcap.org/linux-ntfs/ntfs/concepts/fixup.html
	updateSequenceOffset := parseIntLE(buffer, ntfsEntryUpdateSequenceOffsetOffset, ntfsEntryUpdateSequenceOffsetLength)
	updateSequenceSizeInWords := parseIntLE(buffer, ntfsEntryUpdateSequenceSizeOffset, ntfsEntryUpdateSequenceSizeLength)
	updateSequenceArrayOffset := updateSequenceOffset + ntfsEntryUpdateSequenceNumberLength
	updateSequenceArrayLength := 2*updateSequenceSizeInWords - 2 // see https://flatcap.org/linux-ntfs/ntfs/concepts/file_record.html
	updateSequenceArray := buffer[updateSequenceArrayOffset:updateSequenceArrayOffset+updateSequenceArrayLength]

	for i := int64(0); i < updateSequenceArrayLength/2; i++ {
		buffer[(i+1)*self.sectorSize-2] = updateSequenceArray[i]
		buffer[(i+1)*self.sectorSize-1] = updateSequenceArray[i+1]
	}

	entry := &entry{
		allocatedSize: allocatedSize,
	}

	relativeFirstAttrOffset := parseUintLE(buffer, ntfsEntryFirstAttrOffset, ntfsEntryFirstAttrLength)
	firstAttrOffset := relativeFirstAttrOffset
	attrOffset := firstAttrOffset

	for {
		attrType := parseIntLE(buffer, attrOffset + ntfsAttrTypeOffset, ntfsAttrTypeLength)

		if attrType == ntfsAttrTypeEndMarker {
			break
		}

		attrLen := parseUintLE(buffer, attrOffset + ntfsAttrLengthOffset, ntfsAttrLengthLength)
		if attrLen == 0 { // FIXME this should really never happen!
			break
		}

		if attrType == ntfsAttrTypeData {
			nonResident := parseUintLE(buffer, attrOffset + ntfsAttrDataResidentOffset, ntfsAttrDataResidentLength)

			if nonResident == 0 {
				return nil, ErrDataResident
			}

			dataRealSize := parseUintLE(buffer, attrOffset + ntfsAttrDataRealSizeOffset, ntfsAttrDataRealSizeLength)

/*			if fileSize < 2 * 1024 * 1024 {
				return nil, ErrFileTooSmallToIndex
			}*/

			relativeDataRunsOffset := parseIntLE(buffer, attrOffset + ntfsAttrDataRunsOffset, ntfsAttrDataRunsLength)
			dataRunFirstOffset := attrOffset + int64(relativeDataRunsOffset)

			entry.dataSize = dataRealSize
			entry.runs = self.readRuns(buffer, dataRunFirstOffset)
		}


		attrOffset += attrLen
	}

	if entry.runs == nil {
		return nil, ErrNoDataAttr
	}

	return entry, nil
}

func (self *ntfsDeduper) readRuns(entry []byte, offset int64) []run {
	runs := make([]run, 0)
	firstCluster := int64(0)

	for {
		header := uint64(parseIntLE(entry, offset + ntfsAttrDataRunsHeaderOffset, ntfsAttrDataRunsHeaderLength))

		if header == ntfsAttrDataRunsHeaderEndMarker {
			break
		}

		clusterCountLength := int64(header & 0x0F)  // right nibble
		clusterCountOffset := offset + ntfsAttrDataRunsHeaderLength
		clusterCount := parseUintLE(entry, clusterCountOffset, clusterCountLength)

		firstClusterLength := int64(header & 0xF0 >> 4) // left nibble
		firstClusterOffset := clusterCountOffset + clusterCountLength
		firstCluster += parseIntLE(entry, firstClusterOffset, firstClusterLength) // relative to previous, can be negative, so signed!

		sparse := firstClusterLength == 0

		fromOffset := int64(firstCluster) * int64(self.clusterSize)
		toOffset := fromOffset + int64(clusterCount) * int64(self.clusterSize)

		Printf("data run offset = %d, header = 0x%x, sparse = %t, length length = 0x%x, offset length = 0x%x, " +
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
func (self *ntfsDeduper) dedupFile(entry *entry, chunkmap map[string]*chunk, diskmap map[int64]*chunkPart) {
	remainingToEndOfFile := entry.dataSize

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

			b := make([]byte, self.clusterSize)

			for i := 0; i < int(run.clusterCount); i++ {
				currentChunkChecksum.Write(b)
				remainingToEndOfFile -= self.clusterSize
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

				runBytesRead, err := self.reader.ReadAt(runBuffer[:runBytesMaxToBeRead], runOffset)
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

func (self *ntfsDeduper) index() (*internal.ManifestV1, error) {
	if self.size < ntfsBootRecordSize {
		return nil, errors.New("invalid boot sector, too small")
	}

	// Read NTFS boot sector
	boot := make([]byte, ntfsBootRecordSize)
	_, err := self.reader.ReadAt(boot, 0)
	if err != nil {
		return nil, err
	}

	// Read magic string to ensure this is an NTFS filesystem
	if bytes.Compare([]byte(ntfsBootMagic), boot[ntfsBootMagicOffset:ntfsBootMagicOffset+len(ntfsBootMagic)]) != 0 {
		return nil, errors.New("invalid boot sector, invalid magic")
	}

	// Read basic information
	self.sectorSize = parseUintLE(boot, ntfsBootSectorSizeOffset, ntfsBootSectorSizeLength)
	self.sectorsPerCluster = parseUintLE(boot, ntfsBootSectorsPerClusterOffset, ntfsBootSectorsPerClusterLength)
	self.clusterSize = self.sectorSize * self.sectorsPerCluster

	// Read $MFT entry
	mftClusterNumber := parseUintLE(boot, ntfsBootMftClusterNumberOffset, ntfsBootMftClusterNumberLength)
	mftOffset := mftClusterNumber * self.clusterSize

	Printf("sector size = %d, sectors per cluster = %d, cluster size = %d, mft cluster number = %d, mft offset = %d\n",
		self.sectorSize, self.sectorsPerCluster, self.clusterSize, mftClusterNumber, mftOffset)

	mft, err := self.readEntry(mftOffset)
	if err != nil {
		panic(err)
	}

	fmt.Printf("\n\nMFT: %#v\n\n", mft)

	chunkmap := make(map[string]*chunk, 0)
	diskmap := make(map[int64]*chunkPart, 0)

	for _, run := range mft.runs {
		fmt.Printf("\n\nprocessing run: %#v\n\n", run)
		entries := int(run.clusterCount * self.sectorsPerCluster)
		firstSector := run.firstCluster * self.sectorsPerCluster

		for i := 1; i < entries; i++ { // Skip entry 0 ($MFT itself!)
			sector := firstSector + int64(i)
			sectorOffset := sector * self.sectorSize
			// TODO we should look at entry.allocSize and jump ahead. This will cut the reads in half!

			fmt.Printf("reading entry at sector %d / offset %d:\n", sector, sectorOffset)

			entry, err := self.readEntry(sectorOffset)
			if err == ErrDataResident || err == ErrNoDataAttr || err == ErrUnexpectedMagic {
				fmt.Printf("skipping FILE: %s\n\n", err.Error())
				continue
			} else if err != nil {
				panic(err)
			}

			if entry.dataSize < dedupFileSizeMinBytes {
				fmt.Printf("skipping FILE: too small\n\n")
				continue
			}

			Printf("\n\nFILE %#v\n", entry)
			self.dedupFile(entry, chunkmap, diskmap)
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
		sector := offset / self.sectorSize
		cluster := sector / self.sectorsPerCluster

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
	manifest.Slices = make([]*internal.Slice, 0)

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

	for currentOffset < self.size {
		hasNextBreakpoint := breakpointIndex < len(breakpoints)

		if hasNextBreakpoint {
			breakpoint = breakpoints[breakpointIndex]
			bytesToBreakpoint := breakpoint - currentOffset
			//fmt.Printf("i = %d, len = %d, breakpoint = %d, currentOffset = %d, bytes = %d\n",
			//	breakpointIndex, len(breakpoints), breakpoint, currentOffset, bytesToBreakpoint)

			if bytesToBreakpoint > chunkSizeMaxBytes {
				// FIXME emit chunk

				chunkEndOffset := minInt64(currentOffset + chunkSizeMaxBytes, self.size)
				chunkSize := chunkEndOffset - currentOffset

				// EMIT CHUNK BEGIN
				bytesRead, err := self.reader.ReadAt(chunkBuffer, currentOffset)
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
					bytesRead, err := self.reader.ReadAt(chunkBuffer[:bytesToBreakpoint], currentOffset)
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

			chunkEndOffset := minInt64(currentOffset + chunkSizeMaxBytes, self.size)
			chunkSize := chunkEndOffset - currentOffset

			// EMIT CHUNK BEGIN
			bytesRead, err := self.reader.ReadAt(chunkBuffer[:chunkSize], currentOffset)
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

	return manifest, nil
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

