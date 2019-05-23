package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"heckel.io/fsdup/internal"
	"io"
	"io/ioutil"
	"os"
	"sort"
)

const (
	chunkSizeMaxBytes      = 32 * 1024 * 1024
	dedupFileSizeMinBytes  = 4 * 1024 * 1024

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
	reader            io.ReaderAt
	size              int64
	sectorSize        int64
	sectorsPerCluster int64
	clusterSize       int64
	chunkMap          map[string]bool
	diskMap           map[int64]*chunkPart
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

type fixedChunk struct {
	size int64
	data []byte
	hash hash.Hash
	checksum []byte
}

type chunkPart struct {
	chunk *fixedChunk
	from int64
	to int64
}

var ErrDataResident = errors.New("data is resident")
var ErrNoDataAttr = errors.New("no data attribute")
var ErrUnexpectedMagic = errors.New("unexpected magic")
var ErrFileTooSmallToIndex = errors.New("file too small to index")


func NewNtfsDeduper(reader io.ReaderAt, size int64) *ntfsDeduper {
	return &ntfsDeduper{
		reader:   reader,
		size:     size,
		chunkMap: make(map[string]bool, 0),
		diskMap:  make(map[int64]*chunkPart, 0),
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

		Debugf("data run offset = %d, header = 0x%x, sparse = %t, length length = 0x%x, offset length = 0x%x, " +
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
func (self *ntfsDeduper) dedupFile(entry *entry) error {
	remainingToEndOfFile := entry.dataSize

	chunk := NewChunk()
	os.Mkdir("index", 0770)

	for _, run := range entry.runs {
		Debugf("Processing run at cluster %d, offset %d, cluster count = %d, size = %d\n",
			run.firstCluster, run.fromOffset, run.clusterCount, run.size)

		if run.sparse {
			Debugf("- sparse run, skipping %d bytes\n", self.clusterSize * run.clusterCount)
			remainingToEndOfFile -= self.clusterSize * run.clusterCount
		} else {
			runBuffer := make([]byte, chunkSizeMaxBytes) // buffer cannot be larger than chunkSizeMaxBytes!
			runOffset := run.fromOffset
			runSize := minInt64(remainingToEndOfFile, run.size) // only read to filesize, doesnt always align with clusters!

			remainingToEndOfFile -= runSize
			remainingToEndOfRun := runSize

			for remainingToEndOfRun > 0 {
				remainingToFullChunk := chunk.Remaining()
				runBytesMaxToBeRead := minInt64(minInt64(remainingToEndOfRun, remainingToFullChunk), int64(len(runBuffer)))

				Debugf("- reading disk section at offset %d to max %d bytes (remaining to end of run = %d, remaining to full chunk = %d, run buffer size = %d)\n",
					runOffset, runBytesMaxToBeRead, remainingToEndOfRun, remainingToFullChunk, len(runBuffer))

				runBytesRead, err := self.reader.ReadAt(runBuffer[:runBytesMaxToBeRead], runOffset)
				if err != nil {
					return err
				}

				// Add run to chunk(s)
				if debug {
					Debugf("  - bytes read = %d, current chunk size = %d, chunk max = %d\n",
						runBytesRead, chunk.Size(), chunkSizeMaxBytes)
				}

				self.diskMap[runOffset] = &chunkPart{
					chunk: chunk,
					from: chunk.Size(),
					to: chunk.Size() + int64(runBytesRead),
				}

				chunk.Write(runBuffer[:runBytesRead])

				fmt.Printf("  -> adding %d bytes to chunk, new chunk size is %d\n", runBytesRead, chunk.Size())

				// Emit full chunk, write file and add to chunk map
				if chunk.Full() {
					fmt.Printf("  -> emmitting full chunk %x, size = %d\n", chunk.Checksum(), chunk.Size())

					if _, ok := self.chunkMap[chunk.ChecksumString()]; !ok {
						if err := writeChunkFile(chunk.Checksum(), chunk.Data()); err != nil {
							return err
						}

						self.chunkMap[chunk.ChecksumString()] = true
					}

					chunk.Reset()
				}

				remainingToEndOfRun -= int64(runBytesRead)
				runOffset += int64(runBytesRead)
			}
		}
	}

	// Finish last chunk
	if chunk.Size() > 0 {
		fmt.Printf("  -> emmitting LAST chunk %x, size = %d\n", chunk.Checksum(), chunk.Size())

		if _, ok := self.chunkMap[chunk.ChecksumString()]; !ok {
			if err := writeChunkFile(chunk.Checksum(), chunk.Data()); err != nil {
				return err
			}

			self.chunkMap[chunk.ChecksumString()] = true
		}
	}

	return nil
}

func (self *ntfsDeduper) Dedup() (*internal.ManifestV1, error) {
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

	Debugf("sector size = %d, sectors per cluster = %d, cluster size = %d, mft cluster number = %d, mft offset = %d\n",
		self.sectorSize, self.sectorsPerCluster, self.clusterSize, mftClusterNumber, mftOffset)

	mft, err := self.readEntry(mftOffset)
	if err != nil {
		return nil, err
	}

	Debugf("\n\nMFT: %#v\n\n", mft)

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
				return nil, err
			}

			if entry.dataSize < dedupFileSizeMinBytes {
				fmt.Printf("skipping FILE: too small\n\n")
				continue
			}

			Debugf("\n\nFILE %#v\n", entry)
			self.dedupFile(entry)
		}
	}

	if debug {
		self.printMaps()
	}
	
	println()
	println("manifest:")

	// Create manifest
	breakpoints := make([]int64, 0, len(self.diskMap))
	for breakpoint, _ := range self.diskMap {
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

	chunk := NewChunk()
	buffer := make([]byte, chunkSizeMaxBytes)

	os.Mkdir("index", 0770)

	for currentOffset < self.size {
		hasNextBreakpoint := breakpointIndex < len(breakpoints)

		if hasNextBreakpoint {
			breakpoint = breakpoints[breakpointIndex]
			bytesToBreakpoint := breakpoint - currentOffset

			if bytesToBreakpoint > chunkSizeMaxBytes {
				chunkEndOffset := minInt64(currentOffset + chunkSizeMaxBytes, self.size)

				bytesRead, err := self.reader.ReadAt(buffer, currentOffset)
				if err != nil {
					return nil, err
				} else if bytesRead != chunkSizeMaxBytes {
					return nil, fmt.Errorf("cannot read all bytes from disk, %d read\n", bytesRead)
				}

				chunk.Reset()
				chunk.Write(buffer[:bytesRead])

				if _, ok := self.chunkMap[chunk.ChecksumString()]; !ok {
					if err := writeChunkFile(chunk.Checksum(), chunk.Data()); err != nil {
						return nil, err
					}

					self.chunkMap[chunk.ChecksumString()] = true
				}

				Debugf("offset %d - %d, NEW chunk %x, size %d\n",
					currentOffset, chunkEndOffset, chunk.Checksum(), chunk.Size())

				manifest.Slices = append(manifest.Slices, &internal.Slice{
					Checksum: chunk.Checksum(),
					Offset: 0,
					Length: chunk.Size(),
				})

				currentOffset = chunkEndOffset
			} else {
				if bytesToBreakpoint > 0 {
					// FIXME this should just buffer the current chunk and not emit is right away. It should FILL UP a chunk later!

					bytesRead, err := self.reader.ReadAt(buffer[:bytesToBreakpoint], currentOffset)
					if err != nil {
						return nil, err
					} else if int64(bytesRead) != bytesToBreakpoint {
						return nil, fmt.Errorf("cannot read all bytes from disk, %d read\n", bytesRead)
					}

					chunk.Reset()
					chunk.Write(buffer[:bytesRead])

					if _, ok := self.chunkMap[chunk.ChecksumString()]; !ok {
						if err := writeChunkFile(chunk.Checksum(), chunk.Data()); err != nil {
							return nil, err
						}

						self.chunkMap[chunk.ChecksumString()] = true
					}

					manifest.Slices = append(manifest.Slices, &internal.Slice{
						Checksum: chunk.Checksum(),
						Offset: 0,
						Length: chunk.Size(),
					})

					Debugf("offset %d - %d, NEW2 chunk %x, size %d\n",
						currentOffset, currentOffset + bytesToBreakpoint, chunk.Checksum(), chunk.Size())

					currentOffset += bytesToBreakpoint
				}

				part := self.diskMap[breakpoint]
				chunk := part.chunk
				partSize := part.to - part.from

				Debugf("offset %d - %d, existing chunk %x, offset %d - %d, part size %d\n",
					currentOffset, currentOffset + partSize, chunk.Checksum(), part.from, part.to, partSize)

				manifest.Slices = append(manifest.Slices, &internal.Slice{
					Checksum: chunk.Checksum(),
					Offset: part.from,
					Length: partSize,
				})

				currentOffset += partSize
				breakpointIndex++
			}
		} else {
			chunkEndOffset := minInt64(currentOffset + chunkSizeMaxBytes, self.size)
			chunkSize := chunkEndOffset - currentOffset

			bytesRead, err := self.reader.ReadAt(buffer[:chunkSize], currentOffset)
			if err != nil {
				panic(err)
			} else if int64(bytesRead) != chunkSize {
				panic(fmt.Errorf("cannot read bytes from disk, %d read\n", bytesRead))
			}

			chunk.Reset()
			chunk.Write(buffer[:bytesRead])

			if _, ok := self.chunkMap[chunk.ChecksumString()]; !ok {
				if err := writeChunkFile(chunk.Checksum(), chunk.Data()); err != nil {
					return nil, err
				}

				self.chunkMap[chunk.ChecksumString()] = true
			}

			Debugf("offset %d - %d, NEW3 chunk %x, size %d\n",
				currentOffset, chunkEndOffset, chunk.Checksum(), chunk.Size())

			manifest.Slices = append(manifest.Slices, &internal.Slice{
				Checksum: chunk.Checksum(),
				Offset: 0,
				Length: chunk.Size(),
			})

			currentOffset = chunkEndOffset
		}
	}

	return manifest, nil
}

func (self *ntfsDeduper) printMaps() {
	/*Debugf("Chunk map:\n")
	for _, chunk := range self.chunkMap {
		fmt.Printf("- chunk %x\n", chunk.checksum)
		for _, diskSection := range chunk.diskSections {
			size := diskSection.to - diskSection.from
			Debugf("  - disk section %d - %d, size %d\n", diskSection.from, diskSection.to, size)
		}
	}*/

	Debugf("Disk map:\n")

	for offset, chunkPart := range self.diskMap {
		size := chunkPart.to - chunkPart.from
		sector := offset / self.sectorSize
		cluster := sector / self.sectorsPerCluster

		Debugf("- offset %d - %d (sector %d, cluster %d), chunk %x, offset %d - %d, size %d\n",
			offset, offset + size, sector, cluster, chunkPart.chunk.Checksum(), chunkPart.from, chunkPart.to, size)
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

func NewChunk() *fixedChunk {
	return &fixedChunk{
		size: 0,
		data: make([]byte, chunkSizeMaxBytes),
		hash: sha256.New(),
		checksum: nil,
	}
}

func (c *fixedChunk) Reset() {
	c.size = 0
	c.hash.Reset()
}

func (c *fixedChunk) Write(data []byte) {
	copy(c.data[c.size:c.size+int64(len(data))], data)
	c.hash.Write(data)
	c.checksum = nil // reset, if it's been called before
	c.size += int64(len(data))
}

func (c *fixedChunk) WriteChecksum(data []byte) {
	c.hash.Write(data)
}

func (c *fixedChunk) Checksum() []byte {
	if c.checksum == nil {
		c.checksum = c.hash.Sum(nil)
	}

	return c.checksum
}

func (c *fixedChunk) ChecksumString() string {
	return fmt.Sprintf("%x", c.Checksum())
}

func (c *fixedChunk) Data() []byte {
	return c.data[:c.size]
}

func (c *fixedChunk) Size() int64 {
	return c.size
}

func (c *fixedChunk) Remaining() int64 {
	return int64(len(c.data)) - c.size
}

func (c *fixedChunk) Full() bool {
	return c.Remaining() <= 0
}

