package fsdup

type ChunkStore interface {
	Write(chunk *chunk) error
	ReadAt(checksum []byte, buffer []byte, offset int64) (int, error)
}
