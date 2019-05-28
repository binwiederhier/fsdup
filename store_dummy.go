package main

type dummyChunkStore struct {
	// Hm?
}

func NewDummyStore() *dummyChunkStore {
	return &dummyChunkStore{}
}

func (idx *dummyChunkStore) WriteChunk(chunk *chunk) error {
	return nil
}