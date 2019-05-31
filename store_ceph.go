package main

import (
	"errors"
	"fmt"
	"github.com/ceph/go-ceph/rados"
)

type cephChunkStore struct {
	configFile string
	pool string
	ctx *rados.IOContext
	chunkMap map[string]bool
}

func NewCephStore(configFile string, pool string) *cephChunkStore {
	return &cephChunkStore{
		configFile: configFile,
		pool:       pool,
		chunkMap:   make(map[string]bool, 0),
	}
}

func (idx *cephChunkStore) Write(chunk *chunk) error {
	if err := idx.openPool(); err != nil {
		return err
	}

	checksumStr := chunk.ChecksumString()

	if _, ok := idx.chunkMap[checksumStr]; !ok {
		if _, err := idx.ctx.Stat(checksumStr); err != nil {
			if err := idx.ctx.Write(checksumStr, chunk.Data(), 0); err != nil {
				return err
			}
		}

		idx.chunkMap[chunk.ChecksumString()] = true
	}

	return nil
}

func (idx *cephChunkStore) ReadAt(checksum []byte, buffer []byte, offset int64) (int, error) {
	if err := idx.openPool(); err != nil {
		return 0, err
	}

	checksumStr := fmt.Sprintf("%x", checksum)
	read, err := idx.ctx.Read(checksumStr, buffer, uint64(offset))
	if err != nil {
		return 0, err
	} else if read != len(buffer) {
		return 0, errors.New("cannot read full section")
	}

	return read, nil
}

func (idx *cephChunkStore) openPool() error {
	if idx.ctx != nil {
		return nil
	}

	conn, err := rados.NewConn()
	if err != nil {
		return err
	}

	if err := conn.ReadConfigFile(idx.configFile); err != nil {
		return err
	}

	if err := conn.Connect(); err != nil {
		return err
	}

	idx.ctx, err = conn.OpenIOContext(idx.pool)
	if err != nil {
		return err
	}

	return nil
}
