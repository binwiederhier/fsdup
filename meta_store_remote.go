package fsdup

import (
	"context"
	"google.golang.org/grpc"
	"heckel.io/fsdup/pb"
	"sync"
)

type metaStoreRemote struct {
	serverAddr string
	client pb.HubClient
	sync.Mutex
}

func NewMetaStoreRemote(serverAddr string) *metaStoreRemote {
	return &metaStoreRemote{
		serverAddr: serverAddr,
		client: nil,
	}
}

func (s *metaStoreRemote) GetManifest(manifestId string) (*manifest, error) {
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

func (s *metaStoreRemote) ensureConnected() error {
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