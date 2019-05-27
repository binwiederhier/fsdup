package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
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
)

type indexer interface {
	WriteChunk(chunk *fixedChunk) error
}

type fileIndexer struct {
	root     string
	chunkMap map[string]bool
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

	var manifest *diskManifest

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

func indexMbrDisk(reader io.ReaderAt, offset int64, nowrite bool, exact bool) (*diskManifest, error) {
	chunker := NewMbrDiskChunker(reader, offset, nowrite, exact)
	return chunker.Dedup()
}

func indexNtfs(reader io.ReaderAt, offset int64, nowrite bool, exact bool) (*diskManifest, error) {
	ntfs := NewNtfsChunker(reader, NewFileIndexer("index"), offset, nowrite, exact)
	return ntfs.Dedup()
}

func indexOther(file *os.File, offset int64, nowrite bool) (*diskManifest, error) {
	return nil, nil
}
