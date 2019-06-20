package fsdup

type ChunkStore interface {
	Stat(checksum []byte) error
	ReadAt(checksum []byte, buffer []byte, offset int64) (int, error)
	Write(checksum []byte, buffer []byte) error
	Remove(checksum []byte) error
}
