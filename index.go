package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type fileType int

const (
	typeNtfs fileType = iota + 1
	typeMbrDisk
	typeUnknown
)

const (
	probeTypeBufferLength = 512
)

func index(inputFile string, manifestFile string, offset int64, nowrite bool, exact bool, minSize int64) error {
	file, err := os.Open(inputFile)
	if err != nil {
		return err
	}

	defer file.Close()

	var chunker chunker
	var store chunkStore

	// Determine file size for file or block device
	size := int64(0)

	stat, err := file.Stat()
	if err != nil {
		return errors.New("cannot read file")
	}

	if stat.Mode() & os.ModeDevice == os.ModeDevice {
		// TODO This is ugly, but it works.

		out, err := exec.Command("blockdev", "--getsize64", inputFile).Output()
		if err != nil {
			return err
		}

		size, err = strconv.ParseInt(strings.Trim(string(out), "\n"), 10, 64)
		if err != nil {
			return err
		}
	} else {
		size = stat.Size()
	}

	// Probe type to figure out which chunker to pick
	fileType, err := probeType(file, offset)
	if err != nil {
		return err
	}

	if nowrite {
		store = NewDummyStore()
	} else {
		store = NewFileStore("index")
	}

	switch fileType {
	case typeNtfs:
		chunker = NewNtfsChunker(file, store, offset, exact, minSize)
	case typeMbrDisk:
		chunker = NewMbrDiskChunker(file, store, offset, size, exact, minSize)
	default:
		chunker = NewFixedChunker(file, store, offset, size)
	}

	manifest, err := chunker.Dedup()
	if err != nil {
		return err
	}

	if debug {
		Debugf("Manifest:\n")
		manifest.Print()
	}

	if err := manifest.WriteToFile(manifestFile); err != nil {
		return err
	}

	return nil
}

func probeType(reader io.ReaderAt, offset int64) (fileType, error) {
	buffer := make([]byte, probeTypeBufferLength)
	_, err := reader.ReadAt(buffer, offset)
	if err != nil {
		return -1, err
	}

	// Be aware that the probing order is important.
	// NTFS and GPT also have an MBR signature!

	// Detect NTFS
	if bytes.Compare([]byte(ntfsBootMagic), buffer[ntfsBootMagicOffset:ntfsBootMagicOffset+len(ntfsBootMagic)]) == 0 {
		return typeNtfs, nil
	}

	// Detect MBR
	if mbrSignatureMagic == parseUintLE(buffer, mbrSignatureOffset, mbrSignatureLength) {
		return typeMbrDisk, nil
	}

	return typeUnknown, nil
}
