package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"heckel.io/cephboot/internal"
	"io"
	"math"
	"os"
)

const (
	chunkSizeMaxBytes       = 4 * 1024 * 1024
	dedupFileSizeMinBytes   = 4 * 1024 * 1024
	ntfsMagicOffset         = 3
	ntfsMagic               = "NTFS    "
	ntfsSectorSizeOffset    = 11
	ntfsSectorSizeLength    = 2
	ENTRY_MAGIC             = "FILE"
	ENTRY_FIRST_ATTR_OFFSET = 20
	ENTRY_FIRST_ATTR_LENTGH = 2
	ATTR_TYPE_DATA          = 0x80
	ATTR_TYPE_END_MARKER    = 0xFFFFFF
)

type entry struct {
	fileSize uint64
	runs     []run
}

type run struct {
	sparse bool
	fromOffset   uint64
	toOffset     uint64
	firstCluster int64 // signed!
	clusterCount uint64
	size uint64

	slices []*chunkSlice // in which chunk is the data for this run
}

type chunk struct {
	checksum []byte
	size int64
	slices []*slice
}

type slice struct {
	from uint64
	to uint64
	sparse bool
}

type chunkSlice struct {
	chunk *chunk
	slice
}

var ErrDataResident = errors.New("data is resident")
var ErrNoDataAttr = errors.New("no data attribute")
var ErrUnexpectedMagic = errors.New("unexpected magic")
var ErrFileTooSmallToIndex = errors.New("file too small to index")

func readRuns(reader io.ReaderAt, clusterSize int64, offset int64) []run {
	runs := make([]run, 0)
	firstCluster := int64(0)

	for {
		header := readUintLE(reader, offset, 1)

		if header == 0 {
			break
		}

		clusterCountLength := header & 0x0F
		clusterCountOffset := offset + 1
		clusterCount := readUintLE(reader, clusterCountOffset, int64(clusterCountLength))

		firstClusterLength := header & 0xF0 >> 4
		firstClusterOffset := int64(clusterCountOffset) + int64(clusterCountLength)
		firstCluster += readIntLE(reader, int64(firstClusterOffset), int64(firstClusterLength)) // relative to previous, can be negative, so signed!

		sparse := firstClusterLength == 0

		fromOffset := int64(firstCluster) * int64(clusterSize)
		toOffset := fromOffset + int64(clusterCount) * int64(clusterSize)

		fmt.Printf("data run offset = %d, header = 0x%x, sparse = %b, length length = 0x%x, offset length = 0x%x, " +
			"cluster count = %d, first cluster = %d, from offset = %d, to offset = %d\n",
			offset, header, sparse, clusterCountLength, firstClusterLength, clusterCount, firstCluster,
			fromOffset, toOffset)

		runs = append(runs, run{
			sparse:       sparse,
			firstCluster: firstCluster,
			clusterCount: clusterCount,
			fromOffset:   uint64(fromOffset),
			toOffset:     uint64(toOffset),
			size:         uint64(toOffset) - uint64(fromOffset),
		})

		offset += int64(firstClusterLength) + int64(clusterCountLength) + 1
	}

	return runs
}

func readEntry(reader io.ReaderAt, clusterSize int64, offset int64) (*entry, error) {
	err := readAndCompare(reader, int64(offset), []byte(ENTRY_MAGIC))
	if err != nil {
		return nil, err
	}

	relativeFirstAttrOffset := readUintLE(reader, offset + ENTRY_FIRST_ATTR_OFFSET, ENTRY_FIRST_ATTR_LENTGH)

	entry := &entry{}

	firstAttrOffset := offset + int64(relativeFirstAttrOffset)
	attrOffset := firstAttrOffset

	for {
		attrType := readUintLE(reader, attrOffset, 4)
		attrLen := readUintLE(reader, attrOffset + 4, 4)

		if attrType == ATTR_TYPE_DATA {
			nonResident := readUintLE(reader, attrOffset + 8, 1)

			if nonResident == 0 {
				return nil, ErrDataResident
			}

			dataRealSize := readUintLE(reader, attrOffset + 48, 8)

/*			if fileSize < 2 * 1024 * 1024 {
				return nil, ErrFileTooSmallToIndex
			}*/

			relativeDataRunsOffset := readUintLE(reader, attrOffset + 32, 2)
			dataRunFirstOffset := attrOffset + int64(relativeDataRunsOffset)

			entry.fileSize = dataRealSize
			entry.runs = readRuns(reader, clusterSize, dataRunFirstOffset)
		} else if attrType == ATTR_TYPE_END_MARKER || attrLen == 0 {
			break
		}

		attrOffset += int64(attrLen)
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
 run: mft data run, pointing to sections with data on disk
 chunk: data blob, containing data from one or many runs
 slice: pointer to one or many runs, describing the offsets of how a chunk is assembled
 chunkSlice: pointer to one or many parts of a chunk; a run's data can be spread across multiple chunks

 MFT entry runs:
    [   A     |  B |           C             ]        << run
    [   |  |  |   ||   |   |   |   |   |   | ]        << runBuffer (fixed size)

 Chunks (and slices):
    [   1   |   2    |   3     |    4   |  5 ]        << chunk
    [ a | b | c  | d | e | f |g|  h   |i|  j ]        << chunkSlice

 Result:
  run-to-chunk mapping: needed for the creation of the manifest
  chunk-to-run mapping: needed to be able to assemble/read the chunk (only the FIRST file is necessary)

  run-to-chunk:
   A.chunkSlices = [1: 0-100, 2: 0-20]
   B.chunkSlices = [2: 21-80]
   C.chunkSlices = [2: 81-100, 3: 0-100, 4: 0-100, 5: 0-100]

  chunk-to-run:
   1.slices = [A: 0-80]
   2.slices = [A: 81-100, B: 0-100, C: 0-10]
   3.slices = [C: 11-50]
   4.slices = [C: 51-80]
   5.slices = [C: 81-100]

 */
func dedupFile(fs io.ReaderAt, entry *entry, clusterSize uint64, chunkmap map[string]*chunk, clustermap map[int]*run) {
	remainingToEndOfFile := entry.fileSize

	currentChunkChecksum := sha256.New()
	currentChunk := &chunk{
		slices: make([]*slice, 0),
	}

	currentChunkSlice := &chunkSlice{}
	currentChunkSlice.chunk = currentChunk
	currentChunkSlice.sparse = false
	currentChunkSlice.from = 0

	var lastRun *run

	os.Mkdir("index", 0770)
	f, _ := os.OpenFile("index/temp", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)

	for _, run := range entry.runs {
		lastRun = &run

		fmt.Printf("\nprocessing run at cluster %d, offset %d, cluster count = %d, size = %d\n",
			run.firstCluster, run.fromOffset, run.clusterCount, run.size)

		if run.sparse {
			fmt.Printf("- sparse section\n", run)

			b := make([]byte, clusterSize)

			for i := 0; i < int(run.clusterCount); i++ {
				_, err := f.Write(b)
				if err != nil {
					panic(err)
				}

				currentChunkChecksum.Write(b)
				remainingToEndOfFile -= clusterSize
			}

			currentChunk.slices = append(currentChunk.slices, &slice{
				sparse: true,
				from:   run.fromOffset,
				to:     run.toOffset,
			})
		} else {
			clustermap[int(run.firstCluster)] = &run

			runBuffer := make([]byte, chunkSizeMaxBytes) // buffer cannot be larger than chunkSizeMaxBytes!
			runOffset := run.fromOffset
			runSize := int64(math.Min(float64(remainingToEndOfFile), float64(run.size))) // only read to filesize, doesnt always align with clusters!

			remainingToEndOfFile -= uint64(runSize)
			remainingToEndOfRun := int64(runSize)

			run.slices = make([]*chunkSlice, 0)

			for remainingToEndOfRun > 0 {
				remainingToFullChunk := chunkSizeMaxBytes - currentChunk.size
				runBytesMaxToBeRead := int(math.Min(float64(remainingToEndOfRun), float64(len(runBuffer))))
				runBytesMaxToBeRead = int(math.Min(float64(runBytesMaxToBeRead), float64(remainingToFullChunk)))

				fmt.Printf("- reading section to max %d bytes (remaining to end of run = %d, remaining to full chunk = %d, run buffer size = %d)\n",
					runBytesMaxToBeRead, remainingToEndOfRun, remainingToFullChunk, len(runBuffer))

				runBytesRead, err := fs.ReadAt(runBuffer[:runBytesMaxToBeRead], int64(runOffset))
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

				currentChunkChecksum.Write(runBuffer[:runBytesRead])

				currentChunk.size += int64(runBytesRead)
				currentChunk.slices = append(currentChunk.slices, &slice{
					sparse: false,
					from:   runOffset,
					to:     runOffset + uint64(runBytesRead),
				})

				currentChunkSlice.to = uint64(currentChunk.size)

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

					// Add chunk slice to run
					run.slices = append(run.slices, currentChunkSlice)

					// New chunk
					currentChunkChecksum.Reset()
					currentChunk = &chunk{
						slices: make([]*slice, 0),
					}
					currentChunkSlice = &chunkSlice{}
					currentChunkSlice.chunk = currentChunk
					currentChunkSlice.sparse = false
					currentChunkSlice.from = 0

					f.Close()
					os.Rename("index/temp", "index/" + checksumStr)
					f, _ = os.OpenFile("index/temp", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
				}

				remainingToEndOfRun -= int64(runBytesRead)
				runOffset += uint64(runBytesRead)
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

		// Add chunk slice to run
		if lastRun != nil {
			lastRun.slices = append(lastRun.slices, currentChunkSlice)
		}

		f.Close()
		os.Rename("index/temp", "index/" + checksumStr)
	}


	// algorithm:
	// 1. parse ntfs mft entries
	// 2. read FILE runs into chunks
	// 3. build map cluster->run
	// 4. walk all clusters 0 to X, and create remaining chunks
}

func parseNTFS(filename string) {
	fs, err := os.Open(filename)
	check(err, 1, "cannot open file "+filename)

	defer fs.Close()

	stat, err := fs.Stat()
	if err != nil {
		panic(err)
	}

	err = readAndCompare(fs, ntfsMagicOffset, []byte(ntfsMagic))
	if err != nil {
		panic(err)
	}

	sectorSize := readUintLE(fs, ntfsSectorSizeOffset, ntfsSectorSizeLength)
	sectorsPerCluster := readUintLE(fs, 13, 1)
	clusterSize := sectorSize * sectorsPerCluster
	mftClusterNumber := readUintLE(fs, 48, 8)
	mftOffset := mftClusterNumber * clusterSize

	fmt.Printf("sector size = %d, sectors per cluster = %d, cluster size = %d, mft cluster number = %d, mft offset = %d\n",
		sectorSize, sectorsPerCluster, clusterSize, mftClusterNumber, mftOffset)

	mft, err := readEntry(fs, int64(clusterSize), int64(mftOffset))
	if err != nil {
		panic(err)
	}

	fmt.Printf("\n\nMFT: %#v\n\n", mft)

	chunkmap := make(map[string]*chunk, 0)
	clustermap := make(map[int]*run, 0)

	for _, run := range mft.runs {
		fmt.Printf("\n\nprocessing run: %#v\n\n", run)
		entries := int(run.clusterCount * sectorsPerCluster)
		firstSector := int64(run.firstCluster) * int64(sectorsPerCluster)

		for i := 1; i < entries; i++ { // Skip entry 0 ($MFT itself!)
			sector := firstSector + int64(i)
			sectorOffset := sector * int64(sectorSize)
			// TODO we should look at entry.allocSize and jump ahead. This will cut the reads in half!

			fmt.Printf("reading entry at sector %d / offset %d:\n", sector, sectorOffset)

			entry, err := readEntry(fs, int64(clusterSize), sectorOffset)
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
			dedupFile(fs, entry, clusterSize, chunkmap, clustermap)
		}
	}

	fmt.Println("chunkmap:")
	for _, chunk := range chunkmap {
		fmt.Printf("- chunk %x: %#v\n", chunk.checksum, chunk.slices)
	}

	fmt.Println("\nruns:")
	for cluster, run := range clustermap {
		fmt.Printf("- run at start cluster %d, offset %d - %d\n", cluster, run.fromOffset, run.toOffset)
		for _, slice := range run.slices {
			fmt.Printf("  - chunk %x, offset %d - %d\n", slice.chunk.checksum, slice.from, slice.to)
		}
	}

	m := &internal.ManifestV1{}
	m.Size = uint64(stat.Size())

}