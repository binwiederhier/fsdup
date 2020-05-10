package fsdup

import (
	"errors"
	"os"
)

func Export(manifestId string, store ChunkStore, metaStore MetaStore, outputFile string) error {
	manifest, err := metaStore.ReadManifest(manifestId)
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

	buffer := make([]byte, manifest.chunkMaxSize)
	offset := int64(0)

	for _, breakpoint := range manifest.Offsets() {
		part := manifest.Get(breakpoint)
		sparse := part.checksum == nil
		length := part.chunkto - part.chunkfrom

		if sparse {
			debugf("%013d Skipping sparse section of %d bytes\n", offset, length)
		} else {
			debugf("%013d Writing chunk %x, offset %d - %d (size %d)\n", offset, part.checksum, part.chunkfrom, part.chunkto, length)

			read, err := store.ReadAt(part.checksum, buffer[:length], part.chunkfrom)
			if err != nil {
				return err
			} else if int64(read) != length {
				return errors.New("cannot read all required bytes from chunk")
			}

			written, err := out.WriteAt(buffer[:length], offset)
			if err != nil {
				return err
			} else if int64(written) != length {
				return errors.New("cannot write all bytes to output file")
			}
		}

		offset += length
	}

	err = out.Close()
	if err != nil {
		return err
	}

	return nil
}

// truncateExportFile wipes the output file (truncate to zero, then to target size)
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