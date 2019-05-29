package main

import (
	"fmt"
	"io/ioutil"
	"os"
)

type fileChunkStore struct {
	root     string
	chunkMap map[string]bool
}

func NewFileStore(root string) *fileChunkStore {
	return &fileChunkStore{
		root:     root,
		chunkMap: make(map[string]bool, 0),
	}
}

func (idx *fileChunkStore) WriteChunk(chunk *chunk) error {
	checksum := chunk.ChecksumString()

	if _, ok := idx.chunkMap[checksum]; !ok {
		dir := fmt.Sprintf("%s/%s/%s", idx.root, checksum[0:3], checksum[3:6])
		file := fmt.Sprintf("%s/%s", dir, checksum)

		if _, err := os.Stat(file); err != nil {
			if err := os.MkdirAll(dir, 0770); err != nil {
				return err
			}

			err = ioutil.WriteFile(file, chunk.Data(), 0666)
			if err != nil {
				return err
			}
		}

		idx.chunkMap[chunk.ChecksumString()] = true
	}

	return nil
}
