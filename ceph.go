package main

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"log"
	"math"
)

const chunkSize = 4 * 1024 * 1024

type radosImage struct {
	pool *rados.IOContext
	checksums []string
}

func (d *radosImage) ReadAt(p []byte, off uint) error {
	log.Printf("[radosImage] READ offset:%d len:%d\n", off, len(p))

	fromOffset := int(off)
	toOffset := fromOffset + len(p) - 1
	fromChunkIndex := int(math.Floor(float64(fromOffset) / chunkSize))
	toChunkIndex := int(math.Floor(float64(toOffset) / chunkSize))

	log.Printf("[radosImage] - fromOffset = %d, to = %d, fromChunk = %d, toChunk = %d\n", fromOffset, toOffset,
		fromChunkIndex, toChunkIndex)

	pFrom := 0
	chunkFrom := 0
	chunkTo := 0

	for chunkIndex := fromChunkIndex; chunkIndex <= toChunkIndex; chunkIndex++ {
		chunkChecksum := d.checksums[chunkIndex]

		if chunkIndex == fromChunkIndex {
			chunkFrom = fromOffset % chunkSize
		} else {
			chunkFrom = 0
		}

		if chunkIndex == toChunkIndex {
			chunkTo = toOffset % chunkSize
		} else {
			chunkTo = chunkSize - 1
		}

		chunkPartLen := chunkTo - chunkFrom + 1
		pTo := pFrom + chunkPartLen

		log.Printf("[radosImage] - idx = %d, checksum = %s, chunkFrom = %d, chunkTo = %d, chunkPartLen = %d, pFrom = %d, pTo = %d\n",
			chunkIndex, chunkChecksum, chunkFrom, chunkTo, chunkPartLen, pFrom, pTo)

		read, err := d.pool.Read(chunkChecksum, p[pFrom:pTo], uint64(chunkFrom))
		if err != nil {
			panic(err)
		}
		if read != chunkPartLen {
			panic(fmt.Sprintf("invalid len read. expected %d, but read %d", chunkPartLen, read))
		}

		pFrom += read
	}

	log.Printf("[radosImage] READ offset:%d len:%d\n", off, len(p))
	return nil
}

func (d *radosImage) WriteAt(p []byte, off uint) error {
	//d.file.WriteAt(p, int64(off))
	// TODO NOP
	log.Printf("[radosImage] WRITE offset:%d len:%d\n", off, len(p))
	return nil
}

func (d *radosImage) Disconnect() {
	log.Println("[radosImage] DISCONNECT")
}

func (d *radosImage) Flush() error {
	log.Println("[radosImage] FLUSH")
	return nil
}

func (d *radosImage) Trim(off, length uint) error {
	log.Printf("[radosImage] TRIM offset:%d len:%d\n", off, length)
	return nil
}

func openPool() *rados.IOContext {
	conn, err := rados.NewConn()
	if err != nil {
		panic(err)
	}

	if err := conn.ReadConfigFile("ceph.conf"); err != nil {
		panic(err)
	}

	if err := conn.Connect(); err != nil {
		panic(err)
	}

	pool, err := conn.OpenIOContext("phil1")
	if err != nil {
		panic(err)
	}

	return pool
}
