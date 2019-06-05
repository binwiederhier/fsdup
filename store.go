package fsdup

type ChunkStore interface {
	Write(checksum []byte, buffer []byte) error
	ReadAt(checksum []byte, buffer []byte, offset int64) (int, error)
}
