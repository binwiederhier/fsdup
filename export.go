package main

import (
	"fmt"
	"log"
	"os"
)

func export(manifestFile, outputFile string) error {
	manifest, err := NewManifestFromFile(manifestFile)
	if err != nil {
		return err
	}

	if err := truncateExportFile(outputFile, manifest.Size()); err != nil {
		return err
	}

	// Open file
	out, err := os.OpenFile(outputFile, os.O_WRONLY, 0666)
	if err != nil {
		return err
	}

	buffer := make([]byte, chunkSizeMaxBytes)
	offset := int64(0)

	for _, breakpoint := range manifest.Breakpoints() {
		part := manifest.Get(breakpoint)
		sparse := part.checksum == nil
		length := part.to - part.from

		if sparse {
			Debugf("%013d Skipping sparse section of %d bytes\n", offset, length)
		} else {
			Debugf("%013d Writing chunk %x, offset %d - %d (size %d)\n", offset, part.checksum, part.from, part.to, length)
			chunkFile := fmt.Sprintf("index/%x", part.checksum)

			chunk, err := os.OpenFile(chunkFile, os.O_RDONLY, 0666)
			if err != nil {
				log.Fatalln("Cannot open chunk file:", err)
			}

			read, err := chunk.ReadAt(buffer[:length], part.from)
			if err != nil {
				log.Fatalf("Cannot read chunk %x: %s\n", part.checksum, err.Error())
			} else if int64(read) != length {
				log.Fatalln("Cannot read all required bytes from chunk")
			}

			written, err := out.WriteAt(buffer[:length], offset)
			if err != nil {
				log.Fatalln("Cannot write to output file:", err)
			} else if int64(written) != length {
				log.Fatalln("Cannot write all bytes to output file")
			}

			if err := chunk.Close(); err != nil {
				log.Fatalln("Cannot close file:", err)
			}
		}

		offset += length
	}

	err = out.Close()
	if err != nil {
		log.Fatalln("Cannot close output file")
	}

	return nil
}

// Wipe output file (truncate to zero, then to target size)
func truncateExportFile(outputFile string, size int64) error {
	if _, err := os.Stat(outputFile); err != nil {
		file, err := os.Create(outputFile)
		if err != nil {
			return err
		}

		err = file.Close()
		if err != nil {
			return err
		}
	} else {
		if err := os.Truncate(outputFile, 0); err != nil {
			return err
		}
	}

	if err := os.Truncate(outputFile, size); err != nil {
		return err
	}

	return nil
}