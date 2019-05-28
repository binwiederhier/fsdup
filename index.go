package main

import (
	"bytes"
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

	size, err := readFileSize(file, inputFile)
	if err != nil {
		return err
	}

	// Pick chunk store
	if nowrite {
		store = NewDummyStore()
	} else {
		store = NewFileStore("index")
	}

	// Probe type to figure out which chunker to pick
	fileType, err := probeType(file, offset)
	if err != nil {
		return err
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

// Determine file size for file or block device
func readFileSize(file *os.File, inputFile string) (int64, error) {
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}

	if stat.Mode() & os.ModeDevice == os.ModeDevice {
		// TODO This is ugly, but it works.

		out, err := exec.Command("blockdev", "--getsize64", inputFile).Output()
		if err != nil {
			return 0, err
		}

		size, err := strconv.ParseInt(strings.Trim(string(out), "\n"), 10, 64)
		if err != nil {
			return 0, err
		}

		return size, nil
	} else {
		return stat.Size(), nil
	}
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
