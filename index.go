package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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

type indexer interface {
	WriteChunk(chunk *fixedChunk) error
}

type dummyIndexer struct {

}

type fileIndexer struct {
	root     string
	chunkMap map[string]bool
}

func NewDummyIndexer() *dummyIndexer {
	return &dummyIndexer{}
}

func (idx *dummyIndexer) WriteChunk(chunk *fixedChunk) error {
	return nil
}

func NewFileIndexer(root string) *fileIndexer {
	os.Mkdir(root, 0770)

	return &fileIndexer{
		root:     root,
		chunkMap: make(map[string]bool, 0),
	}
}

func (idx *fileIndexer) WriteChunk(chunk *fixedChunk) error {
	if _, ok := idx.chunkMap[chunk.ChecksumString()]; !ok {
		if err := idx.writeChunkFile(chunk); err != nil {
			return err
		}

		idx.chunkMap[chunk.ChecksumString()] = true
	}

	return nil
}

func (idx *fileIndexer) writeChunkFile(chunk *fixedChunk) error {
	chunkFile := fmt.Sprintf("%s/%x", idx.root, chunk.Checksum())

	if _, err := os.Stat(chunkFile); err != nil {
		err = ioutil.WriteFile(chunkFile, chunk.Data(), 0666)
		if err != nil {
			return err
		}
	}

	return nil
}

func index(inputFile string, manifestFile string, offset int64, nowrite bool, exact bool) error {
	file, err := os.Open(inputFile)
	if err != nil {
		return err
	}

	defer file.Close()

	var index indexer
	var manifest *diskMap

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
		index = NewDummyIndexer()
	} else {
		index = NewFileIndexer("index")
	}

	switch fileType {
	case typeNtfs:
		manifest, err = indexNtfs(file, index, offset, exact)
	case typeMbrDisk:
		manifest, err = indexMbrDisk(file, index, offset, size, exact)
	default:
		manifest, err = indexFixedSize(file, index, offset, size)
	}

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

func indexMbrDisk(reader io.ReaderAt, index indexer, offset int64, size int64, exact bool) (*diskMap, error) {
	chunker := NewMbrDiskChunker(reader, index, offset, size, exact)
	return chunker.Dedup()
}

func indexNtfs(reader io.ReaderAt, index indexer, offset int64, exact bool) (*diskMap, error) {
	ntfs := NewNtfsChunker(reader, index, offset, exact)
	return ntfs.Dedup()
}

func indexFixedSize(reader io.ReaderAt, index indexer, offset int64, size int64) (*diskMap, error) {
	chunker := NewFixedChunker(reader, index, offset, size)
	return chunker.Dedup()
}
