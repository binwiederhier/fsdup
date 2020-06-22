package fsdup

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

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

func maxInt64(a, b int64) int64 {
	if a > b {
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

func convertBytesToHumanReadable(b int64) string {
	const unit = 1024

	if b < unit {
		return fmt.Sprintf("%d byte(s)", b)
	}

	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}