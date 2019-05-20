package main

import (
	"encoding/binary"
	"io"
)

func readIntLE(reader io.ReaderAt, offset int64, length int64) int64 {
	b := make([]byte, length)
	n, err := reader.ReadAt(b, offset)

	if err != nil {
		panic(err)
	} else if n != len(b) {
		panic("cannot read")
	}

	if length == 0 {
		return 0
	}

	// Pad b to 8 bytes (uint64/int64 size), determine if negative
	// and fill the array with the little endian number and the sign
	bpad := make([]byte, 8)
	blen := len(b)

	sign := byte(0)
	if b[blen-1] & 0x80 == 0x80 {
		sign = 0xFF
	}

	for i := 0; i < blen; i++ {
		bpad[i] = b[i]
	}

	for i := blen; i < 8; i++ {
		bpad[i] = sign
	}

	return int64(binary.LittleEndian.Uint64(bpad))
}
