package fsdup

import (
	"fmt"
	"golang.org/x/crypto/blake2b"
)

const (
	DefaultDedupFileSizeMinBytes  = 128 * 1024

	// Max size for any chunk produced by any of the chunkers. Note that lowering this
	// probably has terrible consequences for existing manifests, because the buffers all use this
	// value. Splitting this value and the buffer size would be a way to solve this.
	DefaultChunkSizeMaxBytes = 32 * 1024 * 1024
)

type Chunker interface {
	Dedup() (*manifest, error)
}

type chunk struct {
	size int64
	data []byte
	checksum []byte
}

func NewChunk(maxSize int64) *chunk {
	return &chunk{
		size: 0,
		data: make([]byte, maxSize),
		checksum: nil,
	}
}

func (c *chunk) Reset() {
	c.size = 0
	c.checksum = nil
}

func (c *chunk) Write(data []byte) {
	copy(c.data[c.size:c.size+int64(len(data))], data)
	c.checksum = nil // reset!
	c.size += int64(len(data))
}

func (c *chunk) Checksum() []byte {
	if c.checksum == nil {
		checksum := blake2b.Sum256(c.data[:c.size])
		c.checksum = checksum[:]
	}

	return c.checksum
}

func (c *chunk) ChecksumString() string {
	return fmt.Sprintf("%x", c.Checksum())
}

func (c *chunk) Data() []byte {
	return c.data[:c.size]
}

func (c *chunk) Size() int64 {
	return c.size
}

func (c *chunk) Remaining() int64 {
	return int64(len(c.data)) - c.size
}

func (c *chunk) Full() bool {
	return c.Remaining() <= 0
}

