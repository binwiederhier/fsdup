package fsdup

type ChunkStore interface {
	ReadAt(checksum []byte, buffer []byte, offset int64) (int, error)
	Write(checksum []byte, buffer []byte) error
	Remove(checksum []byte) error
}
