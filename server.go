package fsdup

import (
	"context"
	"google.golang.org/grpc"
	"heckel.io/fsdup/pb"
	"net"
)

func ListenAndServe(address string, store ChunkStore) error {
	srv := &server{
		store: store,
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(128 * 1024 * 1024)) // FIXME
	pb.RegisterHubServer(grpcServer, srv)

	return grpcServer.Serve(listener)
}

type server struct {
	store ChunkStore
}

func (s *server) Diff(ctx context.Context, req *pb.DiffRequest) (*pb.DiffResponse, error) {
	unknownChecksums := make([][]byte, 0)
	for _, checksum := range req.Checksums {
		if err := s.store.Stat(checksum); err != nil {
			unknownChecksums = append(unknownChecksums, checksum)
		}
	}

	return &pb.DiffResponse{
		UnknownChecksums: unknownChecksums,
	}, nil
}


func (s *server) Upload(ctx context.Context, req *pb.UploadRequest) (*pb.UploadResponse, error) {
	err := s.store.Write(req.Checksum, req.Data)
	if err != nil {
		return nil, err
	}

	return &pb.UploadResponse{}, nil
}
