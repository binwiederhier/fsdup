package fsdup

import (
	"errors"
	"fmt"
	"github.com/binwiederhier/buse-go/buse"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
)

// Read:
//       written? ---n---> cached? ---n---> store
//          | y

type manifestImage struct {
	manifest   *manifest
	store      ChunkStore
	target     *os.File
	cache      ChunkStore
	offsets    []int64
	chunks     map[string]*chunk
	written    map[int64]bool
	sliceCount map[string]int64
}

func Map(manifestFile string, store ChunkStore, cache ChunkStore, targetFile string) error {
	manifest, err := NewManifestFromFile(manifestFile)
	if err != nil {
		return err
	}

	var target *os.File
	if targetFile != "" {
		target, err = os.OpenFile(targetFile, os.O_CREATE | os.O_RDWR | os.O_TRUNC, 0600)
		if err != nil {
			return err
		}

		if err := target.Truncate(manifest.Size()); err != nil {
			return err
		}
	}

	deviceName, err := findNextNbdDevice()
	if err != nil {
		return err
	}

	debugf("Creating device %s ...\n", deviceName)

	image := NewManifestImage(manifest, store, cache, target)
	device, err := buse.CreateDevice(deviceName, uint(manifest.Size()), image)
	if err != nil {
		return err
	}

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	go func() {
		if err := device.Connect(); err != nil {
			debugf("Buse device stopped with error: %s", err)
		} else {
			log.Println("Buse device stopped gracefully.")
		}
	}()

	<-sig

	// Received SIGTERM, cleanup
	debugf("SIGINT, disconnecting...\n")
	device.Disconnect()

	return nil
}

func NewManifestImage(manifest *manifest, store ChunkStore, cache ChunkStore, target *os.File) *manifestImage {
	sliceCount := make(map[string]int64, 0)

	// Create slice count map for cache accounting
	for _, sliceOffset := range manifest.Offsets() {
		slice := manifest.Get(sliceOffset)
		if slice.checksum != nil {
			checksumStr := fmt.Sprintf("%x", slice.checksum)

			if _, ok := sliceCount[checksumStr]; ok {
				sliceCount[checksumStr]++
			} else {
				sliceCount[checksumStr] = 1
			}
		}
	}

	return &manifestImage{
		manifest:   manifest,
		store:      store,
		target:     target,
		cache:      cache,
		offsets:    manifest.Offsets(), // cache !
		chunks:     manifest.Chunks(),  // cache !
		written:    make(map[int64]bool, 0),
		sliceCount: sliceCount,
	}
}

func (d *manifestImage) ReadAt(p []byte, off uint) error {
	debugf("READ offset %d, length %d\n", off, len(p))

	if err := d.syncSlices(int64(off), int64(off) + int64(len(p))); err != nil {
		return err
	}

	read, err := d.target.ReadAt(p, int64(off))
	if err != nil {
		return err
	} else if read != len(p) {
		return errors.New("cannot read from target file")
	}

	return nil
}

func (d *manifestImage) WriteAt(p []byte, off uint) error {
	if d.target == nil {
		debugf("Failed to write to device at offset %d, length %d: Cannot write to read only device\n", off, len(p))
		return errors.New("cannot write to read only device")
	}

	if err := d.syncSlices(int64(off), int64(off) + int64(len(p))); err != nil {
		return err
	}

	debugf("WRITE offset:%d len:%d\n", off, len(p))

	written, err := d.target.WriteAt(p, int64(off))
	if err != nil {
		return err
	} else if written != len(p) {
		return errors.New("could not write all bytes")
	}

	return nil
}

func (d *manifestImage) syncSlices(from int64, to int64) error {
	// Find first chunk
	// FIXME Linear search is inefficient; use binary search instead, or something better.
	fromIndex := int64(-1)
	fromOffset := int64(-1)
	toIndex := int64(-1)

	for i := int64(0); i < int64(len(d.offsets)); i++ {
		slice := d.manifest.Get(d.offsets[i])
		sliceStart := d.offsets[i]
		sliceEnd := sliceStart + slice.to - slice.from

		if sliceStart <= from && from < sliceEnd {
			fromIndex = i
			fromOffset = sliceStart
		}

		if sliceStart <= to && to <= sliceEnd { // FIXME: to <= ??
			toIndex = i
			break
		}
	}

	debugf("from index = %d, to index = %d\n", fromIndex, toIndex)
	if fromIndex == -1 || toIndex == -1 {
		return errors.New("out of range read")
	}

	offset := fromOffset
	for i := fromIndex; i <= toIndex; i++ {
		slice := d.manifest.Get(d.offsets[i])
		if err := d.syncSlice(offset, slice); err != nil {
			return err
		}

		offset += slice.to - slice.from
	}

	return nil
}

func (d *manifestImage) syncSlice(offset int64, slice *chunkSlice) error {
	if _, ok := d.written[offset]; ok {
		return nil
	}

	if slice.checksum == nil {
		d.written[offset] = true
		return nil
	}

	length := slice.to - slice.from
	debugf("Syncing diskoff %d - %d (len %d) -> checksum %x, %d to %d\n",
		offset, offset + length, length, slice.checksum, slice.from, slice.to)

	checksumStr := fmt.Sprintf("%x", slice.checksum)
	buffer := make([]byte, chunkSizeMaxBytes) // FIXME: Make this a buffer pool
	read, err := d.cache.ReadAt(slice.checksum, buffer[:length], slice.from)
	if err != nil {
		debugf("Chunk %x not in cache. Retrieving full chunk ...\n", slice.checksum)

		// Read entire chunk, store to cache
		chunk := d.chunks[checksumStr]

		// FIXME: This will fill up the local cache will all chunks and never delete it
		read, err = d.store.ReadAt(slice.checksum, buffer[:chunk.size], 0)
		if err != nil {
			return err
		} else if int64(read) != chunk.size {
			return errors.New(fmt.Sprintf("cannot read entire chunk, read only %d bytes", read))
		}

		if err := d.cache.Write(slice.checksum, buffer[:chunk.size]); err != nil {
			return err
		}

		buffer = buffer[slice.from:slice.to]
	} else if int64(read) != length {
		return errors.New(fmt.Sprintf("cannot read entire slice, read only %d bytes", read))
	}

	_, err = d.target.WriteAt(buffer[:length], offset)
	if err != nil {
		return err
	}

	// Accounting
	d.written[offset] = true
	d.sliceCount[checksumStr]--

	// Remove from cache if it will never be requested again
	if d.sliceCount[checksumStr] == 0 {
		if err := d.cache.Remove(slice.checksum); err != nil {
			return err
		}
	}

	return nil
}

func (d *manifestImage) Disconnect() {
	// No thanks
}

func (d *manifestImage) Flush() error {
	return nil
}

func (d *manifestImage) Trim(off, length uint) error {
	return nil
}

func findNextNbdDevice() (string, error) {
	for i := 0; i < 256; i++ {
		sizeFile := fmt.Sprintf("/sys/class/block/nbd%d/size", i)

		if _, err := os.Stat(sizeFile); err == nil {
			b, err := ioutil.ReadFile(sizeFile)
			if err != nil {
				return "", err
			}

			if strings.Trim(string(b), "\n") == "0" {
				return fmt.Sprintf("/dev/nbd%d", i), nil
			}
		}
	}

	return "", errors.New("cannot find free nbd device, driver not loaded?")
}