package fsdup

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"io/ioutil"
)

type cephChunkStore struct {
	configFile string
	pool string
	compress bool
	ctx *rados.IOContext
	chunkMap map[string]bool
	buffer []byte
}

func NewCephStore(configFile string, pool string, compress bool) *cephChunkStore {
	return &cephChunkStore{
		configFile: configFile,
		pool:       pool,
		compress:   compress,
		chunkMap:   make(map[string]bool, 0),
		buffer:     make([]byte, chunkSizeMaxBytes),
	}
}

func (idx *cephChunkStore) Stat(checksum []byte) error {
	checksumStr := fmt.Sprintf("%x", checksum)

	if _, ok := idx.chunkMap[checksumStr]; ok {
		return nil
	}

	_, err := idx.ctx.Stat(checksumStr)
	return err
}

func (idx *cephChunkStore) ReadAt(checksum []byte, buffer []byte, offset int64) (int, error) {
	if err := idx.openPool(); err != nil {
		return 0, err
	}

	checksumStr := fmt.Sprintf("%x", checksum)

	if idx.compress {
		// FIXME this reads the ENTIRE object, gunzips it, and then only reads the requested section.
		//  don't kill me this is a PoC

		read, err := idx.ctx.Read(checksumStr, idx.buffer, 0)
		if err != nil {
			return 0, err
		}

		reader, err := gzip.NewReader(bytes.NewReader(idx.buffer[:read]))
		if err != nil {
			return 0, err
		}

		decompressed, err := ioutil.ReadAll(reader)
		if err != nil {
			return 0, err
		}

		length := len(buffer)
		end := int64(offset)+int64(length)
		copy(buffer, decompressed[offset:end]) // FIXME urgh ...

		//fmt.Printf("%d - %d = %x\n", from, end, buffer)
		return length, nil

	} else {
		read, err := idx.ctx.Read(checksumStr, buffer, uint64(offset))
		if err != nil {
			return 0, err
		}

		return read, nil
	}
}

func (idx *cephChunkStore) Write(checksum []byte, buffer []byte) error {
	if err := idx.openPool(); err != nil {
		return err
	}

	checksumStr := fmt.Sprintf("%x", checksum)

	if _, ok := idx.chunkMap[checksumStr]; !ok {
		if _, err := idx.ctx.Stat(checksumStr); err != nil {
			if idx.compress {
				var b bytes.Buffer
				writer := gzip.NewWriter(&b)

				if _, err := writer.Write(buffer); err != nil {
					return err
				}

				if err := writer.Close(); err != nil {
					return err
				}

				if err := idx.ctx.Write(checksumStr, b.Bytes(), 0); err != nil {
					return err
				}
			} else {
				if err := idx.ctx.Write(checksumStr, buffer, 0); err != nil {
					return err
				}
			}
		}

		idx.chunkMap[checksumStr] = true
	}

	return nil
}

func (idx *cephChunkStore) Remove(checksum []byte) error {
	if err := idx.openPool(); err != nil {
		return err
	}

	checksumStr := fmt.Sprintf("%x", checksum)
	err := idx.ctx.Delete(checksumStr)
	if err != nil {
		return err
	}

	delete(idx.chunkMap, checksumStr)

	return nil
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
