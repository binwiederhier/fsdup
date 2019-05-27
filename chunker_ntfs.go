package main

import (
	"bytes"
	"errors"
	"io"
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
	ntfsBootTotalSectorsOffset      = 40
	ntfsBootTotalSectorsLength      = 8
	ntfsBootMftClusterNumberOffset  = 48
	ntfsBootMftClusterNumberLength  = 8

	// FILE entry (relative to FILE offset)
	// https://flatcap.org/linux-ntfs/ntfs/concepts/file_record.html
	ntfsEntryMagic                      = "FILE"
	ntfsEntryTypicalSize                = 1024
	ntfsEntryUpdateSequenceOffsetOffset = 4
	ntfsEntryUpdateSequenceOffsetLength = 2
	ntfsEntryUpdateSequenceSizeOffset   = 6
	ntfsEntryUpdateSequenceSizeLength   = 2
	ntfsEntryUpdateSequenceNumberLength = 2
	ntfsEntryAllocatedSizeOffset        = 28
	ntfsEntryAllocatedSizeLength        = 4
	ntfsEntryFirstAttrOffset            = 20
	ntfsEntryFirstAttrLength            = 2
	ntfsEntryFlagsOffset                = 22
	ntfsEntryFlagsLength                = 2
	ntfsEntryFlagInUse                  = 1
	ntfsEntryFlagDir                    = 2

	// $Bitmap file
	ntfsBitmapFileEntryIndex            = 6
	ntfsBitmapMinSparseClusters         = 8 // arbitrary number

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

type ntfsChunker struct {
	reader            io.ReaderAt
	start             int64
	sizeInBytes       int64
	exact             bool
	totalSectors      int64
	sectorSize        int64
	sectorsPerCluster int64
	clusterSize       int64
	index             indexer
	manifest          *diskManifest
}

type entry struct {
	offset        int64
	resident      bool
	data          bool
	inuse         bool
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

var ErrUnexpectedMagic = errors.New("unexpected magic")

func NewNtfsChunker(reader io.ReaderAt, indexer indexer, offset int64, exact bool) *ntfsChunker {
	return &ntfsChunker{
		reader:   reader,
		index:    indexer,
		start:    offset,
		exact:    exact,
		manifest: NewDiskMap(),
	}
}

func (d *ntfsChunker) Dedup() (*diskManifest, error) {
	// Read NTFS boot sector
	boot := make([]byte, ntfsBootRecordSize)
	_, err := d.reader.ReadAt(boot, d.start)
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
	d.totalSectors =  parseUintLE(boot, ntfsBootTotalSectorsOffset, ntfsBootTotalSectorsLength)
	d.clusterSize = d.sectorSize * d.sectorsPerCluster
	d.sizeInBytes = d.sectorSize * d.totalSectors + d.sectorSize // Backup boot sector at the end!

	// Read $MFT entry
	mftClusterNumber := parseUintLE(boot, ntfsBootMftClusterNumberOffset, ntfsBootMftClusterNumberLength)
	mftOffset := mftClusterNumber * d.clusterSize

	Debugf("sector size = %d, sectors per cluster = %d, cluster size = %d, mft cluster number = %d, mft offset = %d\n",
		d.sectorSize, d.sectorsPerCluster, d.clusterSize, mftClusterNumber, mftOffset)

	mft, err := d.readEntry(mftOffset)
	if err != nil {
		return nil, err
	}

	// Find and checksum FILE entries
	if err := d.dedupFiles(mft); err != nil {
		return nil, err
	}

	// Find unused/empty sections based on the $Bitmap
	if !d.exact {
		if err := d.dedupUnused(mft); err != nil {
			return nil, err
		}
	}

	// Dedup the rest (gap areas)
	if err := d.dedupRest(); err != nil {
		return nil, err
	}

	return d.manifest, nil
}

func (d *ntfsChunker) readEntry(offset int64) (*entry, error) {
	err := readAndCompare(d.reader, d.start + offset, []byte(ntfsEntryMagic))
	if err != nil {
		return nil, err
	}

	// Read full entry into buffer (we're guessing size 1024 here)
	buffer := make([]byte, ntfsEntryTypicalSize)
	_, err = d.reader.ReadAt(buffer, d.start + offset)
	if err != nil {
		return nil, err
	}

	// Read entry length, and re-read the buffer if it differs
	allocatedSize := parseUintLE(buffer, ntfsEntryAllocatedSizeOffset, ntfsEntryAllocatedSizeLength)
	if int(allocatedSize) > len(buffer) {
		buffer = make([]byte, allocatedSize)
		_, err = d.reader.ReadAt(buffer, d.start + offset)
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
		offset: offset,
		resident: false,
		data: false,
		inuse: false,
		allocatedSize: allocatedSize,
	}

	// Read flags
	flags := parseUintLE(buffer, ntfsEntryFlagsOffset, ntfsEntryFlagsLength)
	entry.inuse = flags & ntfsEntryFlagInUse == ntfsEntryFlagInUse

	if !entry.inuse {
		return entry, nil
	}

	// Read attributes
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
			entry.resident = nonResident == 0

			if !entry.resident {
				dataRealSize := parseUintLE(buffer, attrOffset + ntfsAttrDataRealSizeOffset, ntfsAttrDataRealSizeLength)

				relativeDataRunsOffset := parseIntLE(buffer, attrOffset + ntfsAttrDataRunsOffset, ntfsAttrDataRunsLength)
				dataRunFirstOffset := attrOffset + int64(relativeDataRunsOffset)

				entry.dataSize = dataRealSize
				entry.runs = d.readRuns(buffer, dataRunFirstOffset)
			}
		}

		attrOffset += attrLen
	}

	entry.data = entry.runs != nil
	return entry, nil
}

func (d *ntfsChunker) dedupFiles(mft *entry) error {
	Debugf("Processing $MFT runs\n")

	for _, run := range mft.runs {
		Debugf("Reading run (from = %d, to = %d, clusters = %d, size = %d, sparse = %t)\n",
			run.fromOffset, run.toOffset, run.clusterCount, run.size, run.sparse)

		startOffset := run.firstCluster * d.sectorsPerCluster * d.sectorSize
		endOffset := startOffset + run.clusterCount * d.sectorsPerCluster * d.sectorSize

		offset := startOffset + mft.allocatedSize // Skip $MFT entry itself!

		for offset < endOffset {
			entry, err := d.readEntry(offset)
			if err == ErrUnexpectedMagic {
				offset += d.sectorSize
				Debugf("Entry at offset %d cannot be read: %s\n", offset, err.Error())
				continue
			} else if err != nil {
				return err
			}

			if !entry.inuse {
				offset += entry.allocatedSize
				Debugf("Entry at offset %d ignored: deleted file\n", offset)
				continue
			}

			if !entry.data {
				offset += entry.allocatedSize
				Debugf("Entry at offset %d ignored: no data attribute\n", offset)
				continue
			}

			if entry.resident {
				offset += entry.allocatedSize
				Debugf("Entry at offset %d ignored: data is resident\n", offset)
				continue
			}

			if entry.dataSize < dedupFileSizeMinBytes {
				offset += entry.allocatedSize
				Debugf("Entry at offset %d skipped: %d byte(s) is too small\n", offset, entry.dataSize)
				continue
			}

			if err := d.dedupFile(entry); err != nil {
				offset += entry.allocatedSize
				Debugf("Entry at offset %d failed to be deduped:\n", offset, err.Error())
				continue
			}

			Debugf("Entry at offset %d successfully indexed\n", offset)
			offset += entry.allocatedSize
		}
	}

	return nil
}

func (d *ntfsChunker) readRuns(entry []byte, offset int64) []run {
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
func (d *ntfsChunker) dedupFile(entry *entry) error {
	Debugf("- Deduping FILE\n")

	remainingToEndOfFile := entry.dataSize

	buffer := make([]byte, chunkSizeMaxBytes) // buffer cannot be larger than chunkSizeMaxBytes; the logic relies on it!
	chunk := NewChunk()
	parts := make(map[int64]*chunkPart, 0)

	for _, run := range entry.runs {
		Debugf("  - Processing run at cluster %d, offset %d, cluster count = %d, size = %d\n",
			run.firstCluster, run.fromOffset, run.clusterCount, run.size)

		if run.sparse {
			Debugf("    -> Sparse run, skipping %d bytes\n", d.clusterSize * run.clusterCount)
			remainingToEndOfFile -= d.clusterSize * run.clusterCount
		} else {
			runOffset := run.fromOffset
			runSize := minInt64(remainingToEndOfFile, run.size) // only read to filesize, doesnt always align with clusters!

			remainingToEndOfFile -= runSize
			remainingToEndOfRun := runSize

			for remainingToEndOfRun > 0 {
				remainingToFullChunk := chunk.Remaining()
				runBytesMaxToBeRead := minInt64(minInt64(remainingToEndOfRun, remainingToFullChunk), int64(len(buffer)))

				Debugf("    -> Reading disk section at offset %d to max %d bytes (remaining to end of run = %d, remaining to full chunk = %d, run buffer size = %d)\n",
					runOffset, runBytesMaxToBeRead, remainingToEndOfRun, remainingToFullChunk, len(buffer))

				runBytesRead, err := d.reader.ReadAt(buffer[:runBytesMaxToBeRead], d.start + runOffset)
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
						d.manifest.Add(partOffset, part)
					}

					parts = make(map[int64]*chunkPart, 0) // clear!

					// Write chunk
					if err := d.index.WriteChunk(chunk); err != nil {
						return err
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
			d.manifest.Add(partOffset, part)
		}

		if err := d.index.WriteChunk(chunk); err != nil {
			return err
		}
	}

	return nil
}

func (d *ntfsChunker) dedupUnused(mft *entry) error {
	// Find $Bitmap entry
	var err error
	bitmap := mft

	for i := 0; i < ntfsBitmapFileEntryIndex; i++ {
		Debugf("reading entry %d\n", bitmap.offset + bitmap.allocatedSize)
		bitmap, err = d.readEntry(bitmap.offset + bitmap.allocatedSize)
		if err != nil {
			return err
		}
	}

	// FIXME This relies solely on the offset. It does not verify that
	//  what we have found is in fact the $Bitmap!

	// Read $Bitmap
	Debugf("$Bitmap is at offset %d\n", bitmap.offset)

	remainingToEndOfFile := bitmap.dataSize
	buffer := make([]byte, d.clusterSize)

	lastWasZero := false
	cluster := int64(0)
	sparseClusterGroupStart := int64(0)
	sparseClusterGroupEnd := int64(0)

	for _, run := range bitmap.runs {
		Debugf("  - Processing run at cluster %d, offset %d, cluster count = %d, size = %d\n",
			run.firstCluster, run.fromOffset, run.clusterCount, run.size)

		runOffset := run.fromOffset
		runSize := minInt64(remainingToEndOfFile, run.size) // only read to filesize, doesnt always align with clusters!

		remainingToEndOfFile -= runSize
		remainingToEndOfRun := runSize

		for remainingToEndOfRun > 0 {
			runBytesMaxToBeRead := minInt64(remainingToEndOfRun, int64(len(buffer)))

			Debugf("    -> Reading disk section at offset %d to max %d bytes (remaining to end of run = %d, run buffer size = %d)\n",
				runOffset, runBytesMaxToBeRead, remainingToEndOfRun, len(buffer))

			runBytesRead, err := d.reader.ReadAt(buffer[:runBytesMaxToBeRead], d.start + runOffset)
			if err != nil {
				return err
			}

			for i := 0; i < runBytesRead; i++ {
				if buffer[i] == 0 {
					if lastWasZero {
						sparseClusterGroupEnd = cluster
					} else {
						lastWasZero = true
						sparseClusterGroupStart = cluster
						sparseClusterGroupEnd = cluster
					}
				} else {
					if lastWasZero {
						lastWasZero = false
						isLargeEnough := (sparseClusterGroupEnd-sparseClusterGroupStart)*8 > ntfsBitmapMinSparseClusters

						if isLargeEnough {
							sparseSectionStartOffset := sparseClusterGroupStart * d.clusterSize * 8
							sparseSectionEndOffset := sparseClusterGroupEnd * d.clusterSize * 8
							sparseSectionLength := sparseSectionEndOffset - sparseSectionStartOffset

							Debugf("- Detected large sparse section %d - %d (%d bytes)\n",
								sparseSectionStartOffset, sparseSectionEndOffset, sparseSectionLength)

							d.manifest.Add(sparseSectionStartOffset, &chunkPart{
								checksum: nil,
								from: sparseSectionStartOffset,
								to: sparseSectionEndOffset,
							})
						}
					}
				}

				cluster++
			}

			remainingToEndOfRun -= int64(runBytesRead)
			runOffset += int64(runBytesRead)
		}
	}

	return nil
}

func (d *ntfsChunker) dedupRest() error {
	chunker := NewFixedChunkerWithSkip(d.reader, d.index, d.start, d.sizeInBytes, d.manifest)

	gapManifest, err := chunker.Dedup()
	if err != nil {
		return err
	}

	d.manifest.Merge(gapManifest)
	return nil
}
