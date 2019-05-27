package main

import (
	"fmt"
	"golang.org/x/crypto/blake2b"
)

type fixedChunk struct {
	size int64
	data []byte
	checksum []byte
}

func NewChunk() *fixedChunk {
	return &fixedChunk{
		size: 0,
		data: make([]byte, chunkSizeMaxBytes),
		checksum: nil,
	}
}

func (c *fixedChunk) Reset() {
	c.size = 0
	c.checksum = nil
}

func (c *fixedChunk) Write(data []byte) {
	copy(c.data[c.size:c.size+int64(len(data))], data)
	c.checksum = nil // reset!
	c.size += int64(len(data))
}

func (c *fixedChunk) Checksum() []byte {
	if c.checksum == nil {
		checksum := blake2b.Sum256(c.data[:c.size])
		c.checksum = checksum[:]
	}

	return c.checksum
}

func (c *fixedChunk) ChecksumString() string {
	return fmt.Sprintf("%x", c.Checksum())
}

func (c *fixedChunk) Data() []byte {
	return c.data[:c.size]
}

func (c *fixedChunk) Size() int64 {
	return c.size
}

func (c *fixedChunk) Remaining() int64 {
	return int64(len(c.data)) - c.size
}

func (c *fixedChunk) Full() bool {
	return c.Remaining() <= 0
}

