package fsdup

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
)

func Import(manifestId string, store ChunkStore, metaStore MetaStore, inputFile string) error {
	manifest, err := metaStore.ReadManifest(manifestId)
	if err != nil {
		return err
	}

	// Open file
	in, err := os.OpenFile(inputFile, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	defer in.Close()

	stat, err := in.Stat()
	if err != nil {
		return err
	} else if stat.Size() != manifest.Size() {
		return errors.New("size in manifest does not match file size. wrong input file?")
	}

	chunkSlices, err := manifest.Slices()
	if err != nil {
		return err
	}

	imported := int64(0)
	skipped := int64(0)
	buffer := make([]byte, manifest.chunkMaxSize)

	for _, checksumStr := range manifest.ChecksumsByDiskOffset(chunkSlices) {
		slices := chunkSlices[checksumStr]

		statusf("Importing chunk %d (%d skipped, %d total) ...", imported + 1, skipped, len(chunkSlices))
		debugf("Importing chunk %s (%d slices) ...\n", checksumStr, len(slices))

		checksum, err := hex.DecodeString(checksumStr) // FIXME this is ugly. checksum should be its own type.
		if err != nil {
			return err
		}

		if err := store.Stat(checksum); err == nil {
			debugf("Skipping chunk. Already exists in index.\n")
			skipped++
			imported++
			continue
		}

		chunkSize := int64(0)

		for i, slice := range slices {
			debugf("idx %-5d diskoff %13d - %13d len %-10d chunkoff %13d - %13d\n",
				i, slice.diskfrom, slice.diskto, slice.length, slice.chunkfrom, slice.chunkto)

			read, err := in.ReadAt(buffer[slice.chunkfrom:slice.chunkto], slice.diskfrom)
			if err != nil {
				return err
			} else if int64(read) != slice.length {
				return errors.New(fmt.Sprintf("cannot read full chunk from input file, read only %d bytes, but %d expectecd", read, slice.length))
			}

			chunkSize += slice.length
		}

		if err := store.Write(checksum, buffer[:chunkSize]); err != nil {
			return err
		}

		imported++
	}

	err = in.Close()
	if err != nil {
		return err
	}

	statusf("Imported %d chunks (%d skipped)\n", imported, skipped)

	return nil
}
