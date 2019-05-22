package main

import (
	"bytes"
	"encoding/binary"
	"io"
)

func parseIntLE(b []byte, offset int64, length int64) int64 {
	bpad := make([]byte, 8)

	// Pad b to 8 bytes (uint64/int64 size), determine if negative
	// and fill the array with the little endian number and the sign
	sign := byte(0)
	if b[offset+length-1] & 0x80 == 0x80 {
		sign = 0xFF
	}

	for i := int64(0); i < length; i++ {
		bpad[i] = b[offset+i]
	}

	for i := length; i < 8; i++ {
		bpad[i] = sign
	}

	return int64(binary.LittleEndian.Uint64(bpad))
}

func parseUintLE(b []byte, offset int64, length int64) int64 {
	bpad := make([]byte, 8)

	for i := int64(0); i < length; i++ {
		bpad[i] = b[offset+i]
	}

	return int64(binary.LittleEndian.Uint64(bpad))
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func readAndCompare(reader io.ReaderAt, offset int64, expected []byte) error {
	actual := make([]byte, len(expected))
	n, err := reader.ReadAt(actual, offset)

	if err != nil {
		return err
	} else if n != len(actual) || bytes.Compare(expected, actual) != 0 {
		return ErrUnexpectedMagic
	}

	return nil
}
