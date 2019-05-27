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
	reader            io.ReaderAt
	index indexer
	start             int64
	sizeInBytes       int64
	exact             bool
}

func NewMbrDiskChunker(reader io.ReaderAt, index indexer, offset int64, exact bool) *mbrDiskChunker {
	return &mbrDiskChunker{
		reader: reader,
		index: index,
		start: offset,
		exact: exact,
	}
}

func (d *mbrDiskChunker) Dedup() (*diskManifest, error) {
	println("i am a disk")
	out := NewManifest()

	buffer := make([]byte, mbrLength)
	_, err := d.reader.ReadAt(buffer, d.start)
	if err != nil {
		return nil, err
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
			ntfs := NewNtfsChunker(d.reader, d.index, d.start, d.exact)
			manifest, err := ntfs.Dedup()
			if err != nil {
				return nil, err
			}

			out.Merge(manifest)
		}
	}

	return out, nil
}
