package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"heckel.io/fsdup/internal"
	"io/ioutil"
	"log"
	"os"
)

func export(manifestFile, outputFile string) {
	in, err := ioutil.ReadFile(manifestFile)
	if err != nil {
		log.Fatalln("Error reading file:", err)
	}

	manifest := &internal.ManifestV1{}
	if err := proto.Unmarshal(in, manifest); err != nil {
		log.Fatalln("Failed to parse address book:", err)
	}

	out, err := os.OpenFile(outputFile, os.O_CREATE | os.O_TRUNC | os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("Failed to open output file:", err)
	}

	buffer := make([]byte, chunkSizeMaxBytes)

	for _, slice := range manifest.Slices {
		chunkFile := fmt.Sprintf("index/%x", slice.Checksum)

		chunk, err := os.OpenFile(chunkFile, os.O_RDONLY, 0666)
		if err != nil {
			log.Fatalln("Cannot open chunk file:", err)
		}

		read, err := chunk.ReadAt(buffer[:slice.Length], slice.Offset)
		if err != nil {
			log.Fatalln("Cannot read chunk:", err)
		} else if int64(read) != slice.Length {
			log.Fatalln("Cannot read all required bytes from chunk")
		}

		written, err := out.Write(buffer[:slice.Length])
		if err != nil {
			log.Fatalln("Cannot write to output file:", err)
		} else if int64(written) != slice.Length {
			log.Fatalln("Cannot write all bytes to output file")
		}
	}

	err = out.Close()
	if err != nil {
		log.Fatalln("Cannot close output file")
	}
}