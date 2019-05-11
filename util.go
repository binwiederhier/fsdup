package main

import (
	"encoding/binary"
	"fmt"
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

func readUintLE(reader io.ReaderAt, offset int64, length int64) uint64 {
	b := make([]byte, length)
	n, err := reader.ReadAt(b, offset)
	if err != nil {
		panic(err)
	} else if n != len(b) {
		panic("cannot read")
	}

	// FIXME All cases are necessary for datarun offsets and lengths!
	switch length {
	case 0:
		return 0
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(b))
	case 4:
		return uint64(binary.LittleEndian.Uint32(b))
	case 8:
		return uint64(binary.LittleEndian.Uint64(b))
	default:
		panic(fmt.Sprintf("invalid length %d", length))
	}
}
