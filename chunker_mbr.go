package main

import (
	"io"
)

const (
	mbrLength                         = 512
	mbrSectorSize                     = 512
	mbrSignatureOffset                = 510
	mbrSignatureLength                = 2
	mbrSignatureMagic                 = 0xaa55 // 0x55aa as little endian
	mbrEntryCount                     = 4
	mbrEntryFirstOffset               = 446
	mbrEntryLength                    = 16
	mbrFirstSectorRelativeOffset      = 8
	mbrEntryFirstSectorRelativeLength = 4
)

type mbrDiskChunker struct {
	reader   io.ReaderAt
	store    chunkStore
	start    int64
	size     int64
	exact    bool
	minSize  int64
	manifest *manifest
}

func NewMbrDiskChunker(reader io.ReaderAt, store chunkStore, offset int64, size int64, exact bool, minSize int64) *mbrDiskChunker {
	return &mbrDiskChunker{
		reader:   reader,
		store:    store,
		start:    offset,
		size:     size,
		exact:    exact,
		minSize:  minSize,
		manifest: NewManifest(),
	}
}

func (d *mbrDiskChunker) Dedup() (*manifest, error) {
	Debugf("Deduping MBR disk ...\n")

	if err := d.dedupNtfsPartitions(); err != nil {
		return nil, err
	}

	if err := d.dedupRest(); err != nil {
		return nil, err
	}

	return d.manifest, nil
}

func (d *mbrDiskChunker) dedupNtfsPartitions() error {
	buffer := make([]byte, mbrLength)
	_, err := d.reader.ReadAt(buffer, d.start)
	if err != nil {
		return err
	}

	for i := int64(0); i < mbrEntryCount; i++ {
		entryOffset := mbrEntryFirstOffset + i * mbrEntryLength

		partitionFirstSector := parseUintLE(buffer, entryOffset+mbrFirstSectorRelativeOffset, mbrEntryFirstSectorRelativeLength)
		partitionOffset := d.start + partitionFirstSector*mbrSectorSize
		Debugf("Reading MBR entry at %d, partition begins at sector %d, offset %d\n",
			entryOffset, partitionFirstSector, partitionOffset)

		if partitionOffset == 0 {
			continue
		}

		partitionType, err := probeType(d.reader, partitionOffset) // TODO fix global func call
		if err != nil {
			continue
		}

		if partitionType == typeNtfs {
			Debugf("NTFS partition found at offset %d\n", partitionOffset)
			ntfs := NewNtfsChunker(d.reader, d.store, partitionOffset, d.exact, d.minSize)
			manifest, err := ntfs.Dedup()
			if err != nil {
				return err
			}

			d.manifest.MergeAtOffset(partitionOffset, manifest)
		}
	}

	return nil
}

func (d *mbrDiskChunker) dedupRest() error {
	chunker := NewFixedChunkerWithSkip(d.reader, d.store, d.start, d.size, d.manifest)

	gapManifest, err := chunker.Dedup()
	if err != nil {
		return err
	}

	d.manifest.Merge(gapManifest)
	return nil
}
