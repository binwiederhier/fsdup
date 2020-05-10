package fsdup

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"heckel.io/fsdup/pb"
	"sync"
)

type remoteChunkStore struct {
	serverAddr string
	client pb.HubClient
	sync.Mutex
}

func NewRemoteChunkStore(serverAddr string) *remoteChunkStore {
	return &remoteChunkStore{
		serverAddr: serverAddr,
		client: nil,
	}
}

func (idx *remoteChunkStore) Stat(checksum []byte) error {
	return nil
}

func (idx *remoteChunkStore) Write(checksum []byte, buffer []byte) error {
	if err := idx.ensureConnected(); err != nil {
		return err
	}

	_, err := idx.client.WriteChunk(context.Background(), &pb.WriteChunkRequest{
		Checksum: checksum,
		Data: buffer,
	})

	return err
}

func (idx *remoteChunkStore) ReadAt(checksum []byte, buffer []byte, offset int64) (int, error) {
	if err := idx.ensureConnected(); err != nil {
		return 0, err
	}

	response, err := idx.client.ReadChunk(context.Background(), &pb.ReadChunkRequest{
		Checksum: checksum,
		Offset: offset,
		Length: int64(len(buffer)),
	})

	if err != nil {
		return 0, err
	}

	if len(buffer) != len(response.Data) {
		return 0, errors.New(fmt.Sprintf("unexpected chunk returned from server, expected %d bytes, but got %d",
			len(buffer), len(response.Data)))
	}

	copied := copy(buffer, response.Data)
	if copied != len(buffer) {
		return copied, errors.New(fmt.Sprintf("could not copy entire response to buffer, only %d bytes copied, but %d bytes requested",
			copied, len(buffer)))
	}

	return copied, nil
}

func (idx *remoteChunkStore) Remove(checksum []byte) error {
	if err := idx.ensureConnected(); err != nil {
		return err
	}

	_, err := idx.client.RemoveChunk(context.Background(), &pb.RemoveChunkRequest{
		Checksum: checksum,
	})

	return err
}


func (idx *remoteChunkStore) ensureConnected() error {
	idx.Lock()
	defer idx.Unlock()

	if idx.client != nil {
		return nil
	}

	conn, err := grpc.Dial(idx.serverAddr, grpc.WithBlock(), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(128 * 1024 * 1024), grpc.MaxCallRecvMsgSize(128 * 1024 * 1024))) // FIXME
	if err != nil {
		return err
	}

	idx.client = pb.NewHubClient(conn)

	return nil
}