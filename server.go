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

	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(128 * 1024 * 1024)) // FIXME
	pb.RegisterHubServer(grpcServer, srv)

	return grpcServer.Serve(listener)
}

type server struct {
	store ChunkStore
	db *sql.DB
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

func (s *server) PutChunk(ctx context.Context, req *pb.PutChunkRequest) (*pb.PutChunkResponse, error) {
	err := s.store.Write(req.Checksum, req.Data)
	if err != nil {
		return nil, err
	}

	return &pb.PutChunkResponse{}, nil
}

func (s *server) PutManifest(ctx context.Context, req *pb.PutManifestRequest) (*pb.PutManifestResponse, error) {
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
	for _, slice := range req.Manifest.Slices {
		if kind(slice.Kind) == kindSparse {
			_, err = stmt.Exec(req.Id, offset, &sql.NullString{},
				slice.Offset, slice.Length, slice.Kind)
		} else {
			_, err = stmt.Exec(req.Id, offset, fmt.Sprintf("%x", slice.Checksum),
				slice.Offset, slice.Length, slice.Kind)
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

	return &pb.PutManifestResponse{}, nil
}

func (s *server) GetManifest(ctx context.Context, req *pb.GetManifestRequest) (*pb.GetManifestResponse, error) {
	manifest := &pb.ManifestV1{
		Slices: make([]*pb.Slice, 0),
	}

	rows, err := s.db.Query(
		"SELECT checksum, chunkOffset, chunkLength, kind FROM manifest WHERE manifestId = ? ORDER BY offset ASC",
		req.Id)
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

		var kind kind
		if kindStr == "file" {
			kind = kindFile
		} else if kindStr == "sparse" {
			kind = kindSparse
		} else {
			kind = kindGap
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

	return &pb.GetManifestResponse{Manifest: manifest}, nil
}