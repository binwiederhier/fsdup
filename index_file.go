package main

import (
	"fmt"
	"io/ioutil"
	"os"
)

type fileChunkIndex struct {
	root     string
	chunkMap map[string]bool
}

func NewFileIndex(root string) *fileChunkIndex {
	os.Mkdir(root, 0770)

	return &fileChunkIndex{
		root:     root,
		chunkMap: make(map[string]bool, 0),
	}
}

func (idx *fileChunkIndex) WriteChunk(chunk *fixedChunk) error {
	if _, ok := idx.chunkMap[chunk.ChecksumString()]; !ok {
		if err := idx.writeChunkFile(chunk); err != nil {
			return err
		}

		idx.chunkMap[chunk.ChecksumString()] = true
	}

	return nil
}

func (idx *fileChunkIndex) writeChunkFile(chunk *fixedChunk) error {
	chunkFile := fmt.Sprintf("%s/%x", idx.root, chunk.Checksum())

	if _, err := os.Stat(chunkFile); err != nil {
		err = ioutil.WriteFile(chunkFile, chunk.Data(), 0666)
		if err != nil {
			return err
		}
	}

	return nil
}
