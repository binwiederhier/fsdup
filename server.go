package fsdup

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"heckel.io/fsdup/pb"
	"net"
)

func ListenAndServe(address string, store ChunkStore) error {
	db, err := sql.Open("mysql", "fsdup:fsdup@/fsdup")
	if err != nil {
		return err
	}

	srv := &server{
		store: store,
		db: db,
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
	db *sql.DB
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
	err := s.store.Remove(request.Checksum)
	if err != nil {
		return nil, err
	}

	return &pb.RemoveChunkResponse{}, nil
}

func (s *server) WriteManifest(ctx context.Context, request *pb.WriteManifestRequest) (*pb.WriteManifestResponse, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO manifest 
		SET 
			manifestId = ?,
			offset = ?,
			checksum = ?,
			chunkOffset = ?,
			chunkLength = ?,
			kind = ?
`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close() // danger!

	offset := int64(0)
	for _, slice := range request.Manifest.Slices {
		if kind(slice.Kind) == kindSparse {
			_, err = stmt.Exec(request.Id, offset, &sql.NullString{},
				slice.Offset, slice.Length, kind(slice.Kind).toString())
		} else {
			_, err = stmt.Exec(request.Id, offset, fmt.Sprintf("%x", slice.Checksum),
				slice.Offset, slice.Length, kind(slice.Kind).toString())
		}
		if err != nil {
			return nil, err
		}
		offset += slice.Length
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return &pb.WriteManifestResponse{}, nil
}

func (s *server) ReadManifest(ctx context.Context, request *pb.ReadManifestRequest) (*pb.ReadManifestResponse, error) {
	manifest := &pb.ManifestV1{
		Slices: make([]*pb.Slice, 0),
	}

	rows, err := s.db.Query(
		"SELECT checksum, chunkOffset, chunkLength, kind FROM manifest WHERE manifestId = ? ORDER BY offset ASC",
		request.Id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var chunkOffset, chunkLength int64
		var kindStr string
		var checksum sql.NullString

		if err := rows.Scan(&checksum, &chunkOffset, &chunkLength, &kindStr); err != nil {
			return nil, err
		}

		kind, err := kindFromString(kindStr)
		if err != nil {
			return nil, err
		}

		if checksum.Valid {
			checksumBytes, err := hex.DecodeString(checksum.String)
			if err != nil {
				return nil, err
			}

			manifest.Slices = append(manifest.Slices, &pb.Slice{
				Checksum: checksumBytes,
				Offset:   chunkOffset,
				Length:   chunkLength,
				Kind:     int32(kind),
			})
		} else {
			manifest.Slices = append(manifest.Slices, &pb.Slice{
				Checksum: nil,
				Offset:   chunkOffset,
				Length:   chunkLength,
				Kind:     int32(kind),
			})
		}
	}

	return &pb.ReadManifestResponse{Manifest: manifest}, nil
}