package fsdup

import (
	"io"
)

const (
	gptSignatureMagic = "EFI PART"
	gptSignatureOffset = 512
	gptHeaderOffset = 512
	gptHeaderLength = 512
	gptLogicalSectorSize = 512
	gptFirstEntrySectorOffset = 72
	gptFirstEntrySectorLength = 8
	gptEntryCountOffset = 80
	gptEntryCountLength = 4
	gptEntrySizeOffset = 84
	gptEntrySizeLength = 4
	gptEntryFirstSectorRelativeOffset = 32
	gptEntryFirstSectorRelativeLength = 8
)

type gptDiskChunker struct {
	reader   io.ReaderAt
	store    ChunkStore
	start    int64
	size     int64
	exact    bool
	noFile   bool
	minSize  int64
	manifest *manifest
}

func NewGptDiskChunker(reader io.ReaderAt, store ChunkStore, offset int64, size int64, exact bool, noFile bool, minSize int64) *gptDiskChunker {
	return &gptDiskChunker{
		reader:   reader,
		store:    store,
		start:    offset,
		size:     size,
		exact:    exact,
		noFile:   noFile,
		minSize:  minSize,
		manifest: NewManifest(),
	}
}

func (d *gptDiskChunker) Dedup() (*manifest, error) {
	statusf("Detected GPT disk ...\n")

	if err := d.dedupNtfsPartitions(); err != nil {
		return nil, err
	}

	if err := d.dedupRest(); err != nil {
		return nil, err
	}

	statusf("GPT disk fully indexed\n")
	return d.manifest, nil
}

func (d *gptDiskChunker) dedupNtfsPartitions() error {
	buffer := make([]byte, gptHeaderLength)
	_, err := d.reader.ReadAt(buffer, d.start + gptHeaderOffset)
	if err != nil {
		return err
	}

	// Read basic information, then re-read buffer
	firstEntrySector := parseUintLE(buffer, gptFirstEntrySectorOffset, gptFirstEntrySectorLength)
	entryCount := parseUintLE(buffer, gptEntryCountOffset, gptEntryCountLength)
	entrySize := parseUintLE(buffer, gptEntrySizeOffset, gptEntrySizeLength)

	buffer = make([]byte, entryCount * entrySize)
	_, err = d.reader.ReadAt(buffer, d.start + firstEntrySector * gptLogicalSectorSize)
	if err != nil {
		return err
	}

	// Walk the entries, index partitions if supported
	for i := int64(0); i < entryCount; i++ {
		entryOffset := i * entrySize

		partitionFirstSector := parseUintLE(buffer, entryOffset+gptEntryFirstSectorRelativeOffset, gptEntryFirstSectorRelativeLength)
		partitionOffset := d.start + partitionFirstSector*gptLogicalSectorSize
		debugf("Reading GPT entry %d, partition begins at sector %d, offset %d\n",
			i+1, partitionFirstSector, partitionOffset)

		if partitionOffset == 0 {
			continue
		}

		partitionType, err := probeType(d.reader, partitionOffset) // TODO fix global func call
		if err != nil {
			continue
		}

		if partitionType == typeNtfs {
			debugf("NTFS partition found at offset %d\n", partitionOffset)
			ntfs := NewNtfsChunker(d.reader, d.store, partitionOffset, d.exact, d.noFile, d.minSize)
			manifest, err := ntfs.Dedup()
			if err != nil {
				return err
			}

			d.manifest.MergeAtOffset(partitionOffset, manifest)
		}
	}

	return nil
}

func (d *gptDiskChunker) dedupRest() error {
	chunker := NewFixedChunkerWithSkip(d.reader, d.store, d.start, d.size, d.manifest)

	gapManifest, err := chunker.Dedup()
	if err != nil {
		return err
	}

	d.manifest.Merge(gapManifest)
	return nil
}
