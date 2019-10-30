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
	chunks     map[string]*chunk
	written    map[int64]bool
	sliceCount map[string]int64
	buffer     []byte
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
		chunks:     manifest.Chunks(),  // cache !
		written:    make(map[int64]bool, 0),
		sliceCount: sliceCount,
		buffer:     make([]byte, manifest.chunkMaxSize),
	}
}

func (d *manifestImage) ReadAt(p []byte, off uint) error {
	debugf("READ offset %d, len %d\n", off, len(p))

	if err := d.syncSlices(int64(off), int64(off) + int64(len(p))); err != nil {
		return d.wrapError(err)
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
		debugf("Failed to write to device at offset %d, to %d: Cannot write to read only device\n", off, len(p))
		return errors.New("cannot write to read only device")
	}

	if err := d.syncSlices(int64(off), int64(off) + int64(len(p))); err != nil {
		return err
	}

	debugf("WRITE offset %d, len %d\n", off, len(p))

	written, err := d.target.WriteAt(p, int64(off))
	if err != nil {
		return err
	} else if written != len(p) {
		return errors.New("could not write all bytes")
	}

	return nil
}

func (d *manifestImage) syncSlices(from int64, to int64) error {
	slices, err := d.manifest.SlicesBetween(from, to)
	if err != nil {
		return d.wrapError(err)
	}

	for _, slice := range slices {
		if err := d.syncSlice(slice); err != nil {
			return err
		}
	}

	return nil
}

func (d *manifestImage) syncSlice(slice *chunkSlice) error {
	if _, ok := d.written[slice.diskfrom]; ok {
		return nil
	}

	if slice.checksum == nil {
		d.written[slice.diskfrom] = true
		return nil
	}

	buffer := d.buffer
	length := slice.chunkto - slice.chunkfrom
	debugf("Syncing diskoff %d - %d (len %d) -> checksum %x, %d to %d\n",
		slice.diskfrom, slice.diskto, length, slice.checksum, slice.chunkfrom, slice.chunkto)

	checksumStr := fmt.Sprintf("%x", slice.checksum)

	read, err := d.cache.ReadAt(slice.checksum, buffer[:length], slice.chunkfrom)
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

		buffer = buffer[slice.chunkfrom:slice.chunkto]
	} else if int64(read) != length {
		return errors.New(fmt.Sprintf("cannot read entire slice, read only %d bytes", read))
	}

	_, err = d.target.WriteAt(buffer[:length], slice.diskfrom)
	if err != nil {
		return err
	}

	// Accounting
	d.written[slice.diskfrom] = true
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

func (d *manifestImage) wrapError(err error) error {
	fmt.Printf("Error: %s\n", err.Error())
	return err
}
