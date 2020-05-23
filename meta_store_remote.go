package fsdup

import (
	"context"
	"google.golang.org/grpc"
	"heckel.io/fsdup/pb"
	"sync"
)

type remoteMetaStore struct {
	serverAddr string
	client pb.HubClient
	sync.Mutex
}

func NewRemoteMetaStore(serverAddr string) *remoteMetaStore {
	debugf("NewRemoteMetaStore(%s)", serverAddr)

	return &remoteMetaStore{
		serverAddr: serverAddr,
		client: nil,
	}
}

func (s *remoteMetaStore) ReadManifest(manifestId string) (*manifest, error) {
	if err := s.ensureConnected(); err != nil {
		return nil, err
	}

	debugf("remoteMetaStore.ReadManifest(%s)", manifestId)

	response, err := s.client.ReadManifest(context.Background(), &pb.ReadManifestRequest{Id: manifestId})
	if err != nil {
		return nil, err
	}

	manifest, err := NewManifestFromProto(response.Manifest)
	if err != nil {
		return nil, err
	}

	return manifest, nil
}

func (s *remoteMetaStore) WriteManifest(manifestId string, manifest *manifest) error {
	if err := s.ensureConnected(); err != nil {
		return err
	}

	debugf("remoteMetaStore.WriteManifest(%s)", manifestId)

	_, err := s.client.WriteManifest(context.Background(), &pb.WriteManifestRequest{Id: manifestId, Manifest: manifest.Proto()})
	if err != nil {
		return err
	}

	return nil
}

func (s *remoteMetaStore) ensureConnected() error {
	s.Lock()
	defer s.Unlock()

	if s.client != nil {
		return nil
	}

	conn, err := grpc.Dial(s.serverAddr, grpc.WithBlock(), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(128 * 1024 * 1024), grpc.MaxCallRecvMsgSize(128 * 1024 * 1024))) // FIXME
	if err != nil {
		return err
	}

	s.client = pb.NewHubClient(conn)

	return nil
}