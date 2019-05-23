package main

import (
	"errors"
	"github.com/golang/protobuf/proto"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func index(inputFile string, manifestFile string, offset int64, nowrite bool) error {
	file, err := os.Open(inputFile)
	if err != nil {
		return err
	}

	defer file.Close()

	// Determine file size for file or block device
	size := int64(0)

	stat, err := file.Stat()
	if err != nil {
		return errors.New("cannot read file")
	}

	if stat.Mode() & os.ModeDevice == os.ModeDevice {
		// TODO This is ugly, but it works.

		out, err := exec.Command("blockdev", "--getsize64", inputFile).Output()
		if err != nil {
			return err
		}

		size, err = strconv.ParseInt(strings.Trim(string(out), "\n"), 10, 64)
		if err != nil {
			return err
		}
	} else {
		size = stat.Size()
	}

	// Determine file type (partition, NTFS, other)
	// ...

	ntfs := NewNtfsDeduper(file, offset, size, nowrite)
	manifest, err := ntfs.Dedup()

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