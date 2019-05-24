package main

import (
	"github.com/golang/protobuf/proto"
	"io/ioutil"
	"log"
	"os"
)

func index(inputFile string, manifestFile string, offset int64, nowrite bool, exact bool) error {
	file, err := os.Open(inputFile)
	if err != nil {
		return err
	}

	defer file.Close()

	// Determine file type (partition, NTFS, other)
	// ...

	ntfs := NewNtfsDeduper(file, offset, nowrite, exact)
	manifest, err := ntfs.Dedup()
	if err != nil {
		log.Fatalln("Failed to dedup:", err)
	}

	if debug {
		Debugf("Manifest:\n")
		printManifest(manifest)
	}

	out, err := proto.Marshal(manifest)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	}
	if err := ioutil.WriteFile(manifestFile, out, 0644); err != nil {
		log.Fatalln("Failed to write address book:", err)
	}

	return nil
}