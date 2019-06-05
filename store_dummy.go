package fsdup

import "errors"

type dummyChunkStore struct {
	// Hm?
}

func NewDummyChunkStore() *dummyChunkStore {
	return &dummyChunkStore{}
}

func (idx *dummyChunkStore) Write(checksum []byte, buffer []byte) error {
	return nil
}

func (idx *dummyChunkStore) ReadAt(checksum []byte, buffer []byte, offset int64) (int, error) {
	return 0, errors.New("cannot read from a dummy store, dummy!")
}