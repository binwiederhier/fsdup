package fsdup

import (
	"context"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"heckel.io/fsdup/pb"
	"net"
)

func ListenAndServe(address string, store ChunkStore, metaStore MetaStore) error {
	srv := &server{
		store: store,
		metaStore: metaStore,
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(grpc.MaxSendMsgSize(128 * 1024 * 1024), grpc.MaxRecvMsgSize(128 * 1024 * 1024)) // FIXME
	pb.RegisterHubServer(grpcServer, srv)

	return grpcServer.Serve(listener)
}

type server struct {
	store ChunkStore
	metaStore MetaStore
}

func (s *server) Diff(ctx context.Context, request *pb.DiffRequest) (*pb.DiffResponse, error) {
	unknownChecksums := make([][]byte, 0)
	for _, checksum := range request.Checksums {
		if err := s.store.Stat(checksum); err != nil {
			unknownChecksums = append(unknownChecksums, checksum)
		}
	}

	return &pb.DiffResponse{
		UnknownChecksums: unknownChecksums,
	}, nil
}

func (s *server) WriteChunk(ctx context.Context, request *pb.WriteChunkRequest) (*pb.WriteChunkResponse, error) {
	err := s.store.Write(request.Checksum, request.Data)
	if err != nil {
		return nil, err
	}

	return &pb.WriteChunkResponse{}, nil
}

func (s *server) ReadChunk(ctx context.Context, request *pb.ReadChunkRequest) (*pb.ReadChunkResponse, error) {
	response := &pb.ReadChunkResponse{
		Data: make([]byte, request.Length),
	}

	_, err := s.store.ReadAt(request.Checksum, response.Data, request.Offset)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (s *server) StatChunk(ctx context.Context, request *pb.StatChunkRequest) (*pb.StatChunkResponse, error) {
	err := s.store.Stat(request.Checksum)

	// FIXME this API is wrong
	if err != nil {
		return &pb.StatChunkResponse{Exists: false}, nil
	} else {
		return &pb.StatChunkResponse{Exists: true}, nil
	}
}


func (s *server) RemoveChunk(ctx context.Context, request *pb.RemoveChunkRequest) (*pb.RemoveChunkResponse, error) {
	debugf("server.RemoveChunk(%x)", request.Checksum)

	err := s.store.Remove(request.Checksum)
	if err != nil {
		return nil, err
	}

	return &pb.RemoveChunkResponse{}, nil
}

func (s *server) WriteManifest(ctx context.Context, request *pb.WriteManifestRequest) (*pb.WriteManifestResponse, error) {
	debugf("server.WriteManifest(%x)", request.Manifest.Id)

	manifest, err := NewManifestFromProto(request.Manifest)
	if err != nil {
		return nil, err
	}

	if err := s.metaStore.WriteManifest(request.Id, manifest); err != nil {
		return nil, err
	}

	return &pb.WriteManifestResponse{}, nil
}

func (s *server) ReadManifest(ctx context.Context, request *pb.ReadManifestRequest) (*pb.ReadManifestResponse, error) {
	manifest, err := s.metaStore.ReadManifest(request.Id)
	if err != nil {
		return nil, err
	}

	return &pb.ReadManifestResponse{Manifest: manifest.Proto()}, nil
}