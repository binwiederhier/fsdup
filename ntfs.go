package main

import (
	"bytes"
	"errors"
	"fmt"
	"golang.org/x/crypto/blake2b"
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
	offset            int64
	size              int64
	nowrite           bool
	exact             bool
	sectorSize        int64
	sectorsPerCluster int64
	clusterSize       int64
	chunkMap          map[string]bool
	diskMap           map[int64]*chunkPart
	manifest          *internal.ManifestV1
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
	checksum []byte
}

type chunkPart struct {
	checksum []byte
	from int64
	to int64
}

var ErrDataResident = errors.New("data is resident")
var ErrNoDataAttr = errors.New("no data attribute")
var ErrUnexpectedMagic = errors.New("unexpected magic")
var ErrFileTooSmallToIndex = errors.New("file too small to index")

func NewNtfsDeduper(reader io.ReaderAt, offset int64, size int64, nowrite bool, exact bool) *ntfsDeduper {
	return &ntfsDeduper{
		reader:   reader,
		offset:   offset,
		size:     size,
		nowrite:  nowrite,
		exact:    exact,
		chunkMap: make(map[string]bool, 0),
		diskMap:  make(map[int64]*chunkPart, 0),
		manifest: &internal.ManifestV1{
			Size:   size,
			Slices: make([]*internal.Slice, 0),
		},
	}
}

func (d *ntfsDeduper) Dedup() (*internal.ManifestV1, error) {
	if d.size < ntfsBootRecordSize {
		return nil, errors.New("invalid boot sector, too small")
	}

	// Read NTFS boot sector
	boot := make([]byte, ntfsBootRecordSize)
	_, err := d.reader.ReadAt(boot, 0)
	if err != nil {
		return nil, err
	}

	// Read magic string to ensure this is an NTFS filesystem
	if bytes.Compare([]byte(ntfsBootMagic), boot[ntfsBootMagicOffset:ntfsBootMagicOffset+len(ntfsBootMagic)]) != 0 {
		return nil, errors.New("invalid boot sector, invalid magic")
	}

	// Read basic information
	d.sectorSize = parseUintLE(boot, ntfsBootSectorSizeOffset, ntfsBootSectorSizeLength)
	d.sectorsPerCluster = parseUintLE(boot, ntfsBootSectorsPerClusterOffset, ntfsBootSectorsPerClusterLength)
	d.clusterSize = d.sectorSize * d.sectorsPerCluster

	// Read $MFT entry
	mftClusterNumber := parseUintLE(boot, ntfsBootMftClusterNumberOffset, ntfsBootMftClusterNumberLength)
	mftOffset := mftClusterNumber * d.clusterSize

	Debugf("sector size = %d, sectors per cluster = %d, cluster size = %d, mft cluster number = %d, mft offset = %d\n",
		d.sectorSize, d.sectorsPerCluster, d.clusterSize, mftClusterNumber, mftOffset)

	mft, err := d.readEntry(mftOffset)
	if err != nil {
		return nil, err
	}

	// Dedup FILE entries, and populate disk map
	if err := d.dedupFiles(mft); err != nil {
		return nil, err
	}
	
	// Dedup the rest (gap areas)
	if err := d.dedupRest(); err != nil {
		return nil, err
	}

	return d.manifest, nil
}

func (d *ntfsDeduper) readEntry(offset int64) (*entry, error) {
	err := readAndCompare(d.reader, offset, []byte(ntfsEntryMagic))
	if err != nil {
		return nil, err
	}

	// Read full entry into buffer (we're guessing size 1024 here)
	buffer := make([]byte, 1024)
	_, err = d.reader.ReadAt(buffer, offset)
	if err != nil {
		return nil, err
	}

	// Read entry length, and re-read the buffer if it differs
	allocatedSize := parseUintLE(buffer, ntfsEntryAllocatedSizeOffset, ntfsEntryAllocatedSizeLength)
	if int(allocatedSize) > len(buffer) {
		buffer = make([]byte, allocatedSize)
		_, err = d.reader.ReadAt(buffer, offset)
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
		buffer[(i+1)*d.sectorSize-2] = updateSequenceArray[i]
		buffer[(i+1)*d.sectorSize-1] = updateSequenceArray[i+1]
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

			relativeDataRunsOffset := parseIntLE(buffer, attrOffset + ntfsAttrDataRunsOffset, ntfsAttrDataRunsLength)
			dataRunFirstOffset := attrOffset + int64(relativeDataRunsOffset)

			entry.dataSize = dataRealSize
			entry.runs = d.readRuns(buffer, dataRunFirstOffset)
		}


		attrOffset += attrLen
	}

	if entry.runs == nil {
		return nil, ErrNoDataAttr
	}

	return entry, nil
}

func (d *ntfsDeduper) dedupFiles(mft *entry) error {
	Debugf("\n\nMFT: %#v\n\n", mft)

	for _, run := range mft.runs {
		Debugf("\n\nprocessing run: %#v\n\n", run)
		entries := int(run.clusterCount * d.sectorsPerCluster)
		firstSector := run.firstCluster * d.sectorsPerCluster

		for i := 1; i < entries; i++ { // Skip entry 0 ($MFT itself!)
			sector := firstSector + int64(i)
			sectorOffset := sector * d.sectorSize
			// TODO we should look at entry.allocSize and jump ahead. This will cut the reads in half!

			Debugf("reading entry at sector %d / offset %d:\n", sector, sectorOffset)

			entry, err := d.readEntry(sectorOffset)
			if err == ErrDataResident || err == ErrNoDataAttr || err == ErrUnexpectedMagic {
				Debugf("skipping FILE: %s\n\n", err.Error())
				continue
			} else if err != nil {
				return err
			}

			if entry.dataSize < dedupFileSizeMinBytes {
				Debugf("skipping FILE: too small\n\n")
				continue
			}

			Debugf("\n\nFILE %#v\n", entry)
			if err := d.dedupFile(entry); err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *ntfsDeduper) readRuns(entry []byte, offset int64) []run {
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

		fromOffset := int64(firstCluster) * int64(d.clusterSize)
		toOffset := fromOffset + int64(clusterCount) * int64(d.clusterSize)

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
func (d *ntfsDeduper) dedupFile(entry *entry) error {
	remainingToEndOfFile := entry.dataSize

	buffer := make([]byte, chunkSizeMaxBytes) // buffer cannot be larger than chunkSizeMaxBytes; the logic relies on it!
	chunk := NewChunk()
	parts := make(map[int64]*chunkPart, 0)

	os.Mkdir("index", 0770)

	for _, run := range entry.runs {
		Debugf("Processing run at cluster %d, offset %d, cluster count = %d, size = %d\n",
			run.firstCluster, run.fromOffset, run.clusterCount, run.size)

		if run.sparse {
			Debugf("- sparse run, skipping %d bytes\n", d.clusterSize * run.clusterCount)
			remainingToEndOfFile -= d.clusterSize * run.clusterCount
		} else {
			runOffset := run.fromOffset
			runSize := minInt64(remainingToEndOfFile, run.size) // only read to filesize, doesnt always align with clusters!

			remainingToEndOfFile -= runSize
			remainingToEndOfRun := runSize

			for remainingToEndOfRun > 0 {
				remainingToFullChunk := chunk.Remaining()
				runBytesMaxToBeRead := minInt64(minInt64(remainingToEndOfRun, remainingToFullChunk), int64(len(buffer)))

				Debugf("- reading disk section at offset %d to max %d bytes (remaining to end of run = %d, remaining to full chunk = %d, run buffer size = %d)\n",
					runOffset, runBytesMaxToBeRead, remainingToEndOfRun, remainingToFullChunk, len(buffer))

				runBytesRead, err := d.reader.ReadAt(buffer[:runBytesMaxToBeRead], runOffset)
				if err != nil {
					return err
				}

				// Add run to chunk(s)
				if debug {
					Debugf("  - bytes read = %d, current chunk size = %d, chunk max = %d\n",
						runBytesRead, chunk.Size(), chunkSizeMaxBytes)
				}

				parts[runOffset] = &chunkPart{
					checksum: nil, // fill this when chunk is finalized!
					from: chunk.Size(),
					to: chunk.Size() + int64(runBytesRead),
				}

				chunk.Write(buffer[:runBytesRead])

				Debugf("  -> adding %d bytes to chunk, new chunk size is %d\n", runBytesRead, chunk.Size())

				// Emit full chunk, write file and add to chunk map
				if chunk.Full() {
					Debugf("  -> emmitting full chunk %x, size = %d\n", chunk.Checksum(), chunk.Size())

					// Add parts to disk map
					for partOffset, part := range parts {
						part.checksum = chunk.Checksum()
						d.diskMap[partOffset] = part
					}

					parts = make(map[int64]*chunkPart, 0) // clear!

					// Write chunk
					if _, ok := d.chunkMap[chunk.ChecksumString()]; !ok {
						if err := d.writeChunkFile(chunk); err != nil {
							return err
						}

						d.chunkMap[chunk.ChecksumString()] = true
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
		Debugf("  -> emmitting LAST chunk %x, size = %d\n", chunk.Checksum(), chunk.Size())

		// Add parts to disk map
		for partOffset, part := range parts {
			part.checksum = chunk.Checksum()
			d.diskMap[partOffset] = part
		}

		if _, ok := d.chunkMap[chunk.ChecksumString()]; !ok {
			if err := d.writeChunkFile(chunk); err != nil {
				return err
			}

			d.chunkMap[chunk.ChecksumString()] = true
		}
	}

	return nil
}

func (d *ntfsDeduper) writeChunkFile(chunk *fixedChunk) error {
	if d.nowrite {
		return nil
	}

	chunkFile := fmt.Sprintf("index/%x", chunk.Checksum())

	if _, err := os.Stat(chunkFile); err != nil {
		err = ioutil.WriteFile(chunkFile, chunk.Data(), 0666)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *ntfsDeduper) dedupRest() error {
	// Sort breakpoints to prepare for sequential disk traversal
	breakpoints := make([]int64, 0, len(d.diskMap))
	for breakpoint, _ := range d.diskMap {
		breakpoints = append(breakpoints, breakpoint)
	}

	sort.Slice(breakpoints, func(i, j int) bool {
		return breakpoints[i] < breakpoints[j]
	})

	// We have determined the breakpoints for the FILE entries.
	// Now, we'll process the non-FILE data to fill in the gaps.

	currentOffset := int64(0)
	breakpointIndex := 0
	breakpoint := int64(0)

	chunk := NewChunk()
	buffer := make([]byte, chunkSizeMaxBytes)

	os.Mkdir("index", 0770)

	for currentOffset < d.size {
		hasNextBreakpoint := breakpointIndex < len(breakpoints)

		if hasNextBreakpoint {
			// At this point, we figure out if the space from the current offset to the
			// next breakpoint will fit in a full chunk.

			breakpoint = breakpoints[breakpointIndex]
			bytesToBreakpoint := breakpoint - currentOffset

			if bytesToBreakpoint > chunkSizeMaxBytes {
				// We can fill an entire chunk, because there are enough bytes to the next breakpoint

				chunkEndOffset := minInt64(currentOffset + chunkSizeMaxBytes, d.size)

				bytesRead, err := d.reader.ReadAt(buffer, currentOffset)
				if err != nil {
					return err
				} else if bytesRead != chunkSizeMaxBytes {
					return fmt.Errorf("cannot read all bytes from disk, %d read\n", bytesRead)
				}

				chunk.Reset()
				chunk.Write(buffer[:bytesRead])

				if _, ok := d.chunkMap[chunk.ChecksumString()]; !ok {
					if err := d.writeChunkFile(chunk); err != nil {
						return err
					}

					d.chunkMap[chunk.ChecksumString()] = true
				}

				Debugf("offset %d - %d, NEW chunk %x, size %d\n",
					currentOffset, chunkEndOffset, chunk.Checksum(), chunk.Size())

				d.manifest.Slices = append(d.manifest.Slices, &internal.Slice{
					Checksum: chunk.Checksum(),
					Offset: 0,
					Length: chunk.Size(),
				})

				currentOffset = chunkEndOffset
			} else {
				// There are NOT enough bytes to the next breakpoint to fill an entire chunk

				if bytesToBreakpoint > 0 {
					// Create and emit a chunk from the current position to the breakpoint.
					// This may create small chunks and is inefficient.
					// FIXME this should just buffer the current chunk and not emit is right away. It should FILL UP a chunk later!

					bytesRead, err := d.reader.ReadAt(buffer[:bytesToBreakpoint], currentOffset)
					if err != nil {
						return err
					} else if int64(bytesRead) != bytesToBreakpoint {
						return fmt.Errorf("cannot read all bytes from disk, %d read\n", bytesRead)
					}

					chunk.Reset()
					chunk.Write(buffer[:bytesRead])

					if _, ok := d.chunkMap[chunk.ChecksumString()]; !ok {
						if err := d.writeChunkFile(chunk); err != nil {
							return err
						}

						d.chunkMap[chunk.ChecksumString()] = true
					}

					d.manifest.Slices = append(d.manifest.Slices, &internal.Slice{
						Checksum: chunk.Checksum(),
						Offset: 0,
						Length: chunk.Size(),
					})

					Debugf("offset %d - %d, NEW2 chunk %x, size %d\n",
						currentOffset, currentOffset + bytesToBreakpoint, chunk.Checksum(), chunk.Size())

					currentOffset += bytesToBreakpoint
				}

				// Now we are AT the breakpoint.
				// Simply add this entry to the manifest.

				part := d.diskMap[breakpoint]
				partSize := part.to - part.from

				Debugf("offset %d - %d, size %d  -> FILE chunk %x, offset %d - %d\n",
					currentOffset, currentOffset + partSize, partSize, part.checksum, part.from, part.to)

				d.manifest.Slices = append(d.manifest.Slices, &internal.Slice{
					Checksum: part.checksum,
					Offset: part.from,
					Length: partSize,
				})

				currentOffset += partSize
				breakpointIndex++
			}
		} else {
			chunkEndOffset := minInt64(currentOffset + chunkSizeMaxBytes, d.size)
			chunkSize := chunkEndOffset - currentOffset

			bytesRead, err := d.reader.ReadAt(buffer[:chunkSize], currentOffset)
			if err != nil {
				panic(err)
			} else if int64(bytesRead) != chunkSize {
				panic(fmt.Errorf("cannot read bytes from disk, %d read\n", bytesRead))
			}

			chunk.Reset()
			chunk.Write(buffer[:bytesRead])

			if _, ok := d.chunkMap[chunk.ChecksumString()]; !ok {
				if err := d.writeChunkFile(chunk); err != nil {
					return err
				}

				d.chunkMap[chunk.ChecksumString()] = true
			}

			Debugf("offset %d - %d, NEW3 chunk %x, size %d\n",
				currentOffset, chunkEndOffset, chunk.Checksum(), chunk.Size())

			d.manifest.Slices = append(d.manifest.Slices, &internal.Slice{
				Checksum: chunk.Checksum(),
				Offset: 0,
				Length: chunk.Size(),
			})

			currentOffset = chunkEndOffset
		}
	}

	return nil
}

func NewChunk() *fixedChunk {
	return &fixedChunk{
		size: 0,
		data: make([]byte, chunkSizeMaxBytes),
		checksum: nil,
	}
}

func (c *fixedChunk) Reset() {
	c.size = 0
	c.checksum = nil
}

func (c *fixedChunk) Write(data []byte) {
	copy(c.data[c.size:c.size+int64(len(data))], data)
	c.checksum = nil // reset!
	c.size += int64(len(data))
}

func (c *fixedChunk) Checksum() []byte {
	if c.checksum == nil {
		checksum := blake2b.Sum256(c.data[:c.size])
		c.checksum = checksum[:]
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

