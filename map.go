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
	manifest    *manifest
	store       ChunkStore
	target      *os.File
	cache       ChunkStore
	breakpoints []int64
	chunks      map[string]*chunk
	written      map[int64]bool
}

func NewManifestImage(manifest *manifest, store ChunkStore, target *os.File) *manifestImage {
	return &manifestImage{
		manifest:    manifest,
		store:       store,
		target:      target,
		cache:       NewFileChunkStore("cache"),
		breakpoints: manifest.Breakpoints(), // cache !
		chunks:      manifest.Chunks(),      // cache !
		written:     make(map[int64]bool),
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

	log.Printf("WRITE offset:%d len:%d\n", off, len(p))

	written, err := d.target.WriteAt(p, int64(off))
	if err != nil {
		return err
	} else if written != len(p) {
		return errors.New("could not write all bytes")
	}

	return nil
}

func (d *manifestImage) syncSlice(offset int64, part *chunkPart) error {
	if _, ok := d.written[offset]; ok {
		return nil
	}

	if part.checksum == nil {
		d.written[offset] = true
		return nil
	}

	length := part.to - part.from
	debugf("Syncing diskoff %d - %d (len %d) -> checksum %x, %d to %d\n",
		offset, offset + length, length, part.checksum, part.from, part.to)

	buffer := make([]byte, length) // FIXME: Make this a buffer pool
	read, err := d.cache.ReadAt(part.checksum, buffer, part.from)
	if err != nil {
		debugf("Chunk %x not in cache. Retrieving full chunk ...\n", part.checksum)

		// Read entire chunk, store to cache
		chunk := d.chunks[fmt.Sprintf("%x", part.checksum)]
		chunk.data = make([]byte, chunk.size)

		// FIXME: This will fill up the local cache will all chunks and never delete it
		read, err = d.store.ReadAt(part.checksum, chunk.data, 0)
		if err := d.cache.Write(chunk); err != nil {
			return err
		} else if int64(read) != chunk.size {
			return errors.New(fmt.Sprintf("cannot read entire chunk, read only %d bytes", read))
		}

		// Copy to target buffer
		copy(buffer, chunk.data[part.from:part.to])
	} else if int64(read) != length {
		return errors.New(fmt.Sprintf("cannot read entire slice, read only %d bytes", read))
	}

	_, err = d.target.WriteAt(buffer, offset)
	if err != nil {
		return err
	} /*else if written != read {
		return errors.New("cannot write all data to target file")
	}*/ // FIXME!

	d.written[offset] = true

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

func (d *manifestImage) syncSlices(from int64, to int64) error {
	// Find first chunk
	// FIXME Linear search is inefficient; use binary search instead, or something better.
	fromIndex := int64(-1)
	fromOffset := int64(-1)
	toIndex := int64(-1)

	for i := int64(0); i < int64(len(d.breakpoints)); i++ {
		part := d.manifest.Get(d.breakpoints[i])
		partStart := d.breakpoints[i]
		partEnd := partStart + part.to - part.from

		if partStart <= from && from < partEnd {
			fromIndex = i
			fromOffset = partStart
		}

		if partStart <= to && to <= partEnd { // FIXME: to <= ??
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
		part := d.manifest.Get(d.breakpoints[i])
		if err := d.syncSlice(offset, part); err != nil {
			return err
		}

		offset += part.to - part.from
	}

	return nil
}

func Map(manifestFile string, store ChunkStore, targetFile string) error {
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

	image := NewManifestImage(manifest, store, target)
	device, err := buse.CreateDevice(deviceName, uint(manifest.Size()), image)
	if err != nil {
		return err
	}

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	go func() {
		if err := device.Connect(); err != nil {
			log.Printf("Buse device stopped with error: %s", err)
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