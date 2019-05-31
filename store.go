package main

import (
	"errors"
	"net/url"
	"regexp"
)

type chunkStore interface {
	Write(chunk *chunk) error
	ReadAt(checksum []byte, buffer []byte, offset int64) (int, error)
}

func createChunkStore(spec string) (chunkStore, error) {
	if regexp.MustCompile(`^ceph:`).MatchString(spec) {
		uri, err := url.ParseRequestURI(spec)
		if err != nil {
			return nil, err
		}

		if uri.Scheme == "ceph" {
			return createCephChunkStore(uri)
		}

		return nil, errors.New("store type not supported")
	}

	return NewFileChunkStore(spec), nil
}

func createCephChunkStore(uri *url.URL) (chunkStore, error) {
	var configFile string
	var pool string

	if uri.Opaque != "" {
		configFile = uri.Opaque
	} else if uri.Path != "" {
		configFile = uri.Path
	} else {
		return nil, errors.New("invalid syntax for ceph store type, should be ceph:FILE?pool=POOL")
	}

	pool = uri.Query().Get("pool")
	if pool == "" {
		return nil, errors.New("invalid syntax for ceph store type, should be ceph:FILE?pool=POOL")
	}

	return NewCephStore(configFile, pool), nil
}
