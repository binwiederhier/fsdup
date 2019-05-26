package main

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"heckel.io/fsdup/internal"
	"io"
	"io/ioutil"
	"log"
	"os"
)

type fileType int

const (
	typeNtfs fileType = iota + 1
	typeMbrDisk
	typeUnknown
)

const (
	probeTypeBufferLength = 512

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

func index(inputFile string, manifestFile string, offset int64, nowrite bool, exact bool) error {
	file, err := os.Open(inputFile)
	if err != nil {
		return err
	}

	defer file.Close()

	manifest := &internal.ManifestV1{}

	fileType, err := probeType(file, offset)
	if err != nil {
		return err
	}

	switch fileType {
	case typeNtfs:
		manifest, err = indexNtfs(file, offset, nowrite, exact)
	case typeMbrDisk:
		manifest, err = indexMbrDisk(file, offset, nowrite, exact)
	default:
		manifest, err = indexOther(file, offset, nowrite)
	}

	if debug {
		Debugf("Manifest:\n")
		printManifest(manifest)
	}

	out, err := proto.Marshal(manifest)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	}
	if err := ioutil.WriteFile(manifestFile, out, 0644); err != nil {
		log.Fatalln("Failed to write address book:", err)
	}

	return nil
}


func probeType(reader io.ReaderAt, offset int64) (fileType, error) {
	buffer := make([]byte, probeTypeBufferLength)
	_, err := reader.ReadAt(buffer, offset)
	if err != nil {
		return -1, err
	}

	// Detect NTFS (note: this also has an MBR signature!)
	if bytes.Compare([]byte(ntfsBootMagic), buffer[ntfsBootMagicOffset:ntfsBootMagicOffset+len(ntfsBootMagic)]) == 0 {
		return typeNtfs, nil
	}

	// Detect MBR
	if mbrSignatureMagic == parseUintLE(buffer, mbrSignatureOffset, mbrSignatureLength) {
		return typeMbrDisk, nil
	}

	return typeUnknown, nil
}

func indexMbrDisk(reader io.ReaderAt, offset int64, nowrite bool, exact bool) (*internal.ManifestV1, error) {
	println("i am a disk")

	buffer := make([]byte, mbrLength)
	_, err := reader.ReadAt(buffer, offset)
	if err != nil {
		return nil, err
	}

	for i := int64(0); i < mbrEntryCount; i++ {
		entryOffset := mbrEntryFirstOffset + i * mbrEntryLength

		partitionFirstSector := parseUintLE(buffer, entryOffset+mbrFirstSectorRelativeOffset, mbrEntryFirstSectorRelativeLength)
		partitionOffset := offset + partitionFirstSector*mbrSectorSize
		Debugf("Reading MBR entry at %d, partition begins at sector %d, offset %d\n",
			entryOffset, partitionFirstSector, partitionOffset)

		if partitionOffset == 0 {
			continue
		}

		partitionType, err := probeType(reader, partitionOffset)
		if err != nil {
			continue
		}

		if partitionType == typeNtfs {
			Debugf("NTFS partition found at offset %d\n", partitionOffset)
			return indexNtfs(reader, partitionOffset, nowrite, exact)
		}
	}

	return nil, nil
}

func indexNtfs(reader io.ReaderAt, offset int64, nowrite bool, exact bool) (*internal.ManifestV1, error) {
	ntfs := NewNtfsDeduper(reader, offset, nowrite, exact)
	manifest, err := ntfs.Dedup()
	if err != nil {
		return nil, err
	}

	return manifest, nil
}

func indexOther(file *os.File, offset int64, nowrite bool) (*internal.ManifestV1, error) {
	return nil, nil
}
