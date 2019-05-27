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

	// Wipe output file (truncate to zero, then to target size)
	if _, err := os.Stat(outputFile); err != nil {
		file, err := os.Create(outputFile)
		if err != nil {
			log.Fatalln("Cannot create file:", err)
		}

		err = file.Close()
		if err != nil {
			log.Fatalln("Cannot close file:", err)
		}
	} else {
		if err := os.Truncate(outputFile, 0); err != nil {
			log.Fatalln("Failed to truncate output file:", err)
		}
	}

	if err := os.Truncate(outputFile, manifest.Size); err != nil {
		log.Fatalln("Failed to truncate output file to correct size:", err)
	}

	// Open file
	out, err := os.OpenFile(outputFile, os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("Failed to open output file:", err)
	}

	buffer := make([]byte, chunkSizeMaxBytes)
	offset := int64(0)

	for _, slice := range manifest.Slices {
		sparse := slice.Checksum == nil

		if sparse {
			Debugf("%013d Skipping sparse section of %d bytes\n", offset, slice.Length)
		} else {
			Debugf("%013d Writing chunk %x, offset %d - %d (size %d)\n", offset, slice.Checksum, slice.Offset, slice.Offset + slice.Length, slice.Length)
			chunkFile := fmt.Sprintf("index/%x", slice.Checksum)

			chunk, err := os.OpenFile(chunkFile, os.O_RDONLY, 0666)
			if err != nil {
				log.Fatalln("Cannot open chunk file:", err)
			}

			read, err := chunk.ReadAt(buffer[:slice.Length], slice.Offset)
			if err != nil {
				log.Fatalf("Cannot read chunk %x: %s\n", slice.Checksum, err.Error())
			} else if int64(read) != slice.Length {
				log.Fatalln("Cannot read all required bytes from chunk")
			}

			written, err := out.WriteAt(buffer[:slice.Length], offset)
			if err != nil {
				log.Fatalln("Cannot write to output file:", err)
			} else if int64(written) != slice.Length {
				log.Fatalln("Cannot write all bytes to output file")
			}

			if err := chunk.Close(); err != nil {
				log.Fatalln("Cannot close file:", err)
			}
		}

		offset += slice.Length
	}

	err = out.Close()
	if err != nil {
		log.Fatalln("Cannot close output file")
	}
}