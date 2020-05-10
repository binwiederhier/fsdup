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
	return &remoteMetaStore{
		serverAddr: serverAddr,
		client: nil,
	}
}

func (s *remoteMetaStore) GetManifest(manifestId string) (*manifest, error) {
	if err := s.ensureConnected(); err != nil {
		return nil, err
	}

	response, err := s.client.GetManifest(context.Background(), &pb.GetManifestRequest{Id: manifestId})
	if err != nil {
		return nil, err
	}

	manifest, err := NewManifestFromProto(response.Manifest)
	if err != nil {
		return nil, err
	}

	return manifest, nil
}

func (s *remoteMetaStore) PutManifest(manifestId string, manifest *manifest) error {
	if err := s.ensureConnected(); err != nil {
		return err
	}

	_, err := s.client.PutManifest(context.Background(), &pb.PutManifestRequest{Id: manifestId, Manifest: manifest.Proto()})
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
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(128 * 1024 * 1024))) // FIXME
	if err != nil {
		return err
	}

	s.client = pb.NewHubClient(conn)

	return nil
}