package fsdup

import (
	"bytes"
	"errors"
	"io"
)

// ntfsChunker reads and deduplicates files within the NTFS
// file system. It implements the chunker interface.
//
// Assuming the input is an NTFS partition, it works in
// three passes:
//
// Pass 1: Find FILE entries > $minSize according to $MFT (F):
//  _____________________________________________
// |__|FFF|_____|FFF|_______|FF|_________|FF|____|
//
// Pass 2: Find unused sections according to $Bitmap and mark as sparse (S):
//  _____________________________________________
// |__|FFF|_|SS||FFF|__|SS|_|FF|___|SSSSS|FF|____|
//
// Pass 3: Find gaps:
//  _____________________________________________
// |GG|FFF|G|SS||FFF|GG|SS|G|FF|GGG|SSSSS|FF|GGGG|
//
//
// Glossary:
//   run:  mft data run, pointing to chunkParts with data on disk
//   chunk: data blob, containing data from one or many runs
//   chunk part: pointer to one or many parts of a chunk; a run's data can be spread across multiple chunks
//
type ntfsChunker struct {
	reader            io.ReaderAt
	start             int64
	sizeInBytes       int64
	exact             bool
	minSize           int64
	totalSectors      int64
	sectorSize        int64
	sectorsPerCluster int64
	clusterSize       int64
	store             ChunkStore
	buffer            []byte // cannot be larger than chunkSizeMaxBytes, the logic relies on it!
	chunk             *chunk

	// Output manifest
	out               *manifest
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

const (
	chunkSizeMaxBytes      = 32 * 1024 * 1024

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

	// $Bitmap file
	ntfsBitmapFileEntryIndex            = 6
	ntfsBitmapMinSparseClusters         = 0 // arbitrary number
	ntfsBitsPerByte                     = 8

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

var ErrUnexpectedMagic = errors.New("unexpected magic")

func NewNtfsChunker(reader io.ReaderAt, store ChunkStore, offset int64, exact bool, minSize int64) *ntfsChunker {
	return &ntfsChunker{
		reader:  reader,
		store:   store,
		start:   offset,
		exact:   exact,
		minSize: minSize,
		chunk:   NewChunk(),
		buffer:  make([]byte, chunkSizeMaxBytes),
		out:     NewManifest(),
	}
}

func (d *ntfsChunker) Dedup() (*manifest, error) {
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

	statusf("NTFS partition of size %s found at offset %d\n", convertBytesToHumanReadable(d.sizeInBytes), d.start)

	// Read $MFT entry
	mftClusterNumber := parseUintLE(boot, ntfsBootMftClusterNumberOffset, ntfsBootMftClusterNumberLength)
	mftOffset := mftClusterNumber * d.clusterSize

	debugf("sector size = %d, sectors per cluster = %d, cluster size = %d, mft cluster number = %d, mft offset = %d\n",
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
	if err := d.dedupGaps(); err != nil {
		return nil, err
	}

	statusf("NTFS partition successfully indexed\n")

	return d.out, nil
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
	statusf("Reading NTFS $MFT ...")

	processedEntries := int64(0)
	dedupedEntries := int64(0)
	totalFileSize := int64(0)
	maxEntries := int64(0)

	for _, run := range mft.runs {
		maxEntries += run.clusterCount * d.sectorsPerCluster * d.sectorSize / mft.allocatedSize
	}

	for _, run := range mft.runs {
		debugf("Reading run (from = %d, to = %d, clusters = %d, size = %d, sparse = %t)\n",
			run.fromOffset, run.toOffset, run.clusterCount, run.size, run.sparse)

		startOffset := run.firstCluster * d.sectorsPerCluster * d.sectorSize
		endOffset := startOffset + run.clusterCount * d.sectorsPerCluster * d.sectorSize

		offset := startOffset + mft.allocatedSize // Skip $MFT entry itself!

		for offset < endOffset {
			processedEntries++

			entry, err := d.readEntry(offset)
			if err == ErrUnexpectedMagic {
				offset += d.sectorSize
				debugf("Entry at offset %d cannot be read: %s\n", offset, err.Error())
				continue
			} else if err != nil {
				return err
			}

			if !entry.inuse {
				offset += entry.allocatedSize
				debugf("Entry at offset %d ignored: deleted file\n", offset)
				continue
			}

			if !entry.data {
				offset += entry.allocatedSize
				debugf("Entry at offset %d ignored: no data attribute\n", offset)
				continue
			}

			if entry.resident {
				offset += entry.allocatedSize
				debugf("Entry at offset %d ignored: data is resident\n", offset)
				continue
			}

			if entry.dataSize < d.minSize {
				offset += entry.allocatedSize
				debugf("Entry at offset %d skipped: %d byte(s) is too small\n", offset, entry.dataSize)
				continue
			}

			statusf("Indexing file entry %d/%d (%s / %d indexed, %d skipped) ...",
				processedEntries, maxEntries, convertBytesToHumanReadable(totalFileSize), dedupedEntries, processedEntries - dedupedEntries)

			bytesIndexed, err := d.dedupFile(entry)
			if err != nil {
				offset += entry.allocatedSize
				debugf("Entry at offset %d failed to be deduped:\n", offset, err.Error())
				continue
			}

			debugf("Entry at offset %d successfully indexed\n", offset)

			offset += entry.allocatedSize
			totalFileSize += bytesIndexed
			dedupedEntries++
		}
	}

	statusf("Indexed %s in %d file(s) (%d skipped)\n",
		convertBytesToHumanReadable(totalFileSize), dedupedEntries, processedEntries - dedupedEntries)

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

		debugf("data run offset = %d, header = 0x%x, sparse = %t, length length = 0x%x, offset length = 0x%x, " +
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

func (d *ntfsChunker) dedupFile(entry *entry) (int64, error) {
	remainingToEndOfFile := entry.dataSize
	bytesIndexed := int64(0)

	slices := make(map[int64]*chunkSlice, 0)
	d.chunk.Reset()

	for _, run := range entry.runs {
		debugf("- Processing run at cluster %d, offset %d, cluster count = %d, size = %d\n",
			run.firstCluster, run.fromOffset, run.clusterCount, run.size)

		if run.sparse {
			debugf("- Sparse run, skipping %d bytes\n", d.clusterSize * run.clusterCount)
			remainingToEndOfFile -= d.clusterSize * run.clusterCount
		} else {
			runOffset := run.fromOffset
			runSize := minInt64(remainingToEndOfFile, run.size) // only read to filesize, doesnt always align with clusters!

			bytesIndexed += runSize
			remainingToEndOfFile -= runSize
			remainingToEndOfRun := runSize

			for remainingToEndOfRun > 0 {
				remainingToFullChunk := d.chunk.Remaining()
				runBytesMaxToBeRead := minInt64(minInt64(remainingToEndOfRun, remainingToFullChunk), int64(len(d.buffer)))

				debugf("- Reading disk section at offset %d to max %d bytes (remaining to end of run = %d, remaining to full chunk = %d, run buffer size = %d)\n",
					runOffset, runBytesMaxToBeRead, remainingToEndOfRun, remainingToFullChunk, len(d.buffer))

				runBytesRead, err := d.reader.ReadAt(d.buffer[:runBytesMaxToBeRead], d.start + runOffset)
				if err != nil {
					return 0, err
				}

				// Add run to chunk(s)
				debugf("- Bytes read = %d, current chunk size = %d, chunk max = %d\n",
					runBytesRead, d.chunk.Size(), chunkSizeMaxBytes)

				slices[runOffset] = &chunkSlice{
					checksum: nil, // fill this when chunk is finalized!
					from: d.chunk.Size(),
					to: d.chunk.Size() + int64(runBytesRead),
					kind: kindFile,
				}

				d.chunk.Write(d.buffer[:runBytesRead])

				debugf("- Adding %d bytes to chunk, new chunk size is %d\n", runBytesRead, d.chunk.Size())

				// Emit full chunk, write file and add to chunk map
				if d.chunk.Full() {
					debugf("- Chunk full. Emitting chunk %x, size = %d\n", d.chunk.Checksum(), d.chunk.Size())

					// Add slices to disk map
					for sliceOffset, slice := range slices {
						slice.checksum = d.chunk.Checksum()
						debugf("- Adding disk section %d - %d, mapping to chunk %x, offset %d - %d\n",
							sliceOffset, sliceOffset+ slice.to - slice.from, slice.checksum, slice.from, slice.to)
						d.out.Add(sliceOffset, slice)
					}

					slices = make(map[int64]*chunkSlice, 0) // clear!

					// Write chunk
					if err := d.store.Write(d.chunk.Checksum(), d.chunk.Data()); err != nil {
						return 0, err
					}

					d.chunk.Reset()
				}

				remainingToEndOfRun -= int64(runBytesRead)
				runOffset += int64(runBytesRead)
			}

			// Add sparse section for files that are not cluster-aligned (most files!)
			// FIXME This works but is untested!
			if !d.exact {
				if runOffset % d.sectorSize != 0 {
					remainingToEndOfCluster := d.sectorSize - runOffset % d.sectorSize
					debugf("- File end is not cluster aligned, emitting sparse section %d - %d\n",
						runOffset, runOffset + remainingToEndOfCluster)

					d.out.Add(runOffset, &chunkSlice{
						checksum: nil,
						from: 0,
						to: remainingToEndOfCluster,
						kind: kindSparse,
					})
				}
			}
		}
	}

	// Finish last chunk
	if d.chunk.Size() > 0 {
		// Add slices to disk map
		for sliceOffset, slice := range slices {
			slice.checksum = d.chunk.Checksum()
			debugf("- Adding disk section %d - %d, mapping to chunk %x, offset %d - %d\n",
				sliceOffset, sliceOffset+ slice.to - slice.from, slice.checksum, slice.from, slice.to)
			d.out.Add(sliceOffset, slice)
		}

		debugf("- End of file. Emitting last chunk %x, size = %d\n", d.chunk.Checksum(), d.chunk.Size())
		if err := d.store.Write(d.chunk.Checksum(), d.chunk.Data()); err != nil {
			return 0, err
		}
	}

	return bytesIndexed, nil
}

// dedupUnused reads the NTFS $Bitmap file to find unused clusters and
// creates sparse entry in the manifest for them.
//
// The logic is a little simplified right now, as it treats the bit-map
// as a byte-map, only looking at 8 empty clusters in a row (= 8 bits, 1 byte).
func (d *ntfsChunker) dedupUnused(mft *entry) error {
	statusf("Indexing unused space ...")

	// Find $Bitmap entry
	var err error
	bitmap := mft

	for i := 0; i < ntfsBitmapFileEntryIndex; i++ {
		debugf("reading entry %d\n", bitmap.offset + bitmap.allocatedSize)
		bitmap, err = d.readEntry(bitmap.offset + bitmap.allocatedSize)
		if err != nil {
			return err
		}
	}

	// FIXME This relies solely on the offset. It does not verify that
	//  what we have found is in fact the $Bitmap!

	// Read $Bitmap
	debugf("$Bitmap is at offset %d\n", bitmap.offset)

	sparseBytes := int64(0)

	remainingToEndOfFile := bitmap.dataSize
	buffer := make([]byte, d.clusterSize)

	lastWasZero := false
	cluster := int64(0)
	sparseClusterGroupStart := int64(0)
	sparseClusterGroupEnd := int64(0)

	for _, run := range bitmap.runs {
		debugf("  - Processing run at cluster %d, offset %d, cluster count = %d, size = %d\n",
			run.firstCluster, run.fromOffset, run.clusterCount, run.size)

		runOffset := run.fromOffset
		runSize := minInt64(remainingToEndOfFile, run.size) // only read to filesize, doesnt always align with clusters!

		remainingToEndOfFile -= runSize
		remainingToEndOfRun := runSize

		for remainingToEndOfRun > 0 {
			runBytesMaxToBeRead := minInt64(remainingToEndOfRun, int64(len(buffer)))

			debugf("    -> Reading disk section at offset %d to max %d bytes (remaining to end of run = %d, run buffer size = %d)\n",
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
						isLargeEnough := (sparseClusterGroupEnd-sparseClusterGroupStart)*ntfsBitsPerByte > ntfsBitmapMinSparseClusters

						if isLargeEnough {
							sparseSectionStartOffset := sparseClusterGroupStart * d.clusterSize * ntfsBitsPerByte
							sparseSectionEndOffset := sparseClusterGroupEnd * d.clusterSize * ntfsBitsPerByte
							sparseSectionLength := sparseSectionEndOffset - sparseSectionStartOffset

							debugf("- Detected large sparse section %d - %d (%d bytes)\n",
								sparseSectionStartOffset, sparseSectionEndOffset, sparseSectionLength)

							d.out.Add(sparseSectionStartOffset, &chunkSlice{
								checksum: nil,
								from: sparseSectionStartOffset,
								to: sparseSectionEndOffset,
								kind: kindSparse,
							})

							sparseBytes += sparseSectionLength
							statusf("Finding unused space via $Bitmap (%s marked sparse) ...", convertBytesToHumanReadable(sparseBytes))
						}
					}
				}

				cluster++
			}

			remainingToEndOfRun -= int64(runBytesRead)
			runOffset += int64(runBytesRead)
		}
	}

	statusf("Indexed %s of unused space\n", convertBytesToHumanReadable(sparseBytes))

	return nil
}

func (d *ntfsChunker) dedupGaps() error {
	chunker := NewFixedChunkerWithSkip(d.reader, d.store, d.start, d.sizeInBytes, d.out)

	gapManifest, err := chunker.Dedup()
	if err != nil {
		return err
	}

	d.out.Merge(gapManifest)
	return nil
}
