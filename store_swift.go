package fsdup

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/ncw/swift"
)

type swiftChunkStore struct {
	connection *swift.Connection
	container  string
	chunkMap   map[string]bool
	buffer     []byte
}

func NewSwiftStore(connection *swift.Connection, container string) *swiftChunkStore {
	return &swiftChunkStore{
		connection: connection,
		container:  container,
		chunkMap:   make(map[string]bool, 0),
		buffer:     make([]byte, chunkSizeMaxBytes),
	}
}

func (idx *swiftChunkStore) Stat(checksum []byte) error {
	if err := idx.openConnection(); err != nil {
		return err
	}

	checksumStr := fmt.Sprintf("%x", checksum)

	if _, ok := idx.chunkMap[checksumStr]; ok {
		return nil
	}

	_, _, err := idx.connection.Object(idx.container, checksumStr)
	return err
}

func (idx *swiftChunkStore) ReadAt(checksum []byte, buffer []byte, offset int64) (int, error) {
	if err := idx.openConnection(); err != nil {
		return 0, err
	}

	checksumStr := fmt.Sprintf("%x", checksum)

	requestHeaders := make(swift.Headers)
	requestHeaders["Range"] = fmt.Sprintf("bytes=%d-%d", offset, len(buffer)-1)

	var responseBuffer bytes.Buffer
	_, err := idx.connection.ObjectGet(idx.container, checksumStr, &responseBuffer, false, requestHeaders)
	if err != nil {
		return 0, err
	}

	if responseBuffer.Len() != len(buffer) {
		return 0, errors.New(fmt.Sprintf("cannot read %d chunk bytes, response was %s bytes instead", len(buffer), responseBuffer.Len()))
	}

	copied := copy(buffer, responseBuffer.Bytes())
	if copied != len(buffer) {
		return 0, errors.New(fmt.Sprintf("cannot copy %d chunk bytes, only %s bytes copied instead", len(buffer), copied))
	}

	return len(buffer), nil
}

func (idx *swiftChunkStore) Write(checksum []byte, buffer []byte) error {
	if err := idx.openConnection(); err != nil {
		return err
	}

	checksumStr := fmt.Sprintf("%x", checksum)

	if _, ok := idx.chunkMap[checksumStr]; !ok {
		if err := idx.connection.ObjectPutBytes(idx.container, checksumStr, buffer, "application/x-fsdup-chunk"); err != nil {
			return err
		}

		idx.chunkMap[checksumStr] = true
	}

	return nil
}

func (idx *swiftChunkStore) Remove(checksum []byte) error {
	if err := idx.openConnection(); err != nil {
		return err
	}

	checksumStr := fmt.Sprintf("%x", checksum)
	err := idx.connection.ObjectDelete(idx.container, checksumStr)
	if err != nil {
		return err
	}

	delete(idx.chunkMap, checksumStr)

	return nil
}

func (idx *swiftChunkStore) openConnection() error {
	if idx.connection.Authenticated() {
		return nil
	}

	err := idx.connection.Authenticate()
	if err != nil {
		return err
	}

	return nil
}
