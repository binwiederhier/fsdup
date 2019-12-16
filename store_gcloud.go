package fsdup

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"io"
	"sync"
)

type gcloudStore struct {
	projectID  string
	bucketName string

	client   *storage.Client
	bucket   *storage.BucketHandle
	chunkMap *sync.Map
}

func NewGcloudStore(projectID string, bucket string) *gcloudStore {
	return &gcloudStore{
		projectID:  projectID,
		bucketName: bucket,
		chunkMap:   &sync.Map{},
	}
}

func (idx *gcloudStore) Stat(checksum []byte) error {
	if err := idx.createClient(); err != nil {
		return err
	}

	checksumStr := fmt.Sprintf("%x", checksum)

	if _, ok := idx.chunkMap.Load(checksumStr); ok {
		return nil
	}

	object := idx.bucket.Object(checksumStr)
	_, err := object.Attrs(context.Background())

	return err
}

func (idx *gcloudStore) ReadAt(checksum []byte, buffer []byte, offset int64) (int, error) {
	if err := idx.createClient(); err != nil {
		return 0, err
	}

	checksumStr := fmt.Sprintf("%x", checksum)
	object := idx.bucket.Object(checksumStr)
	reader, err := object.NewRangeReader(context.Background(), offset, offset + int64(len(buffer)))
	if err != nil {
		return 0, err
	}

	read, err := io.ReadFull(reader, buffer)
	if err != nil {
		return read, err
	}

	if err := reader.Close(); err != nil {
		return 0, err
	}

	return read, nil
}

func (idx *gcloudStore) Write(checksum []byte, buffer []byte) error {
	if err := idx.createClient(); err != nil {
		return err
	}

	checksumStr := fmt.Sprintf("%x", checksum)

	if _, ok := idx.chunkMap.Load(checksumStr); !ok {
		object := idx.bucket.Object(checksumStr)
		if _, err := object.Attrs(context.Background()); err != nil {
			writer := object.NewWriter(context.Background())
			if _, err := writer.Write(buffer); err != nil {
				return err
			}

			if err := writer.Close(); err != nil {
				return err
			}
		}

		idx.chunkMap.Store(checksumStr, true)
	}

	return nil
}

func (idx *gcloudStore) Remove(checksum []byte) error {
	if err := idx.createClient(); err != nil {
		return err
	}

	checksumStr := fmt.Sprintf("%x", checksum)
	object := idx.bucket.Object(checksumStr)
	if err := object.Delete(context.Background()); err != nil {
		return err
	}

	idx.chunkMap.Delete(checksumStr)

	return nil
}

func (idx *gcloudStore) createClient() error {
	if idx.client != nil {
		return nil
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	bucket := client.Bucket(idx.bucketName)
	_, err = bucket.Attrs(ctx)
	if err == storage.ErrBucketNotExist {
		if err := bucket.Create(ctx, idx.projectID, nil); err != nil {
			return err
		}
	}

	idx.client = client
	idx.bucket = bucket

	return nil
}
