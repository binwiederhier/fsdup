package main

type dummyChunkIndex struct {
	// Hm?
}

func NewDummyIndex() *dummyChunkIndex {
	return &dummyChunkIndex{}
}

func (idx *dummyChunkIndex) WriteChunk(chunk *fixedChunk) error {
	return nil
}