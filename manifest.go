package main

import (
	"github.com/golang/protobuf/proto"
	"heckel.io/fsdup/internal"
	"io/ioutil"
)

func readManifestFromFile(manifestFile string) (*internal.ManifestV1, error) {
	in, err := ioutil.ReadFile(manifestFile)
	if err != nil {
		return nil, err
	}

	manifest := &internal.ManifestV1{}
	if err := proto.Unmarshal(in, manifest); err != nil {
		return nil, err
	}

	return manifest, nil
}
