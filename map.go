package fsdup

import (
	"errors"
	"fmt"
	"gitlab.datto.net/pheckel/copy-on-demand/copyondemand"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"
)

type manifestImage struct {
	manifest    *manifest
	store       ChunkStore
	cache       ChunkStore
	cacheExpiry *sync.Map
	chunks      map[string]*chunk
	sliceCount  map[string]int64
	buffer      []byte
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

	source := &copyondemand.SyncReaderAt{Reader: image, Size: uint64(manifest.Size())}
	backingFile := &copyondemand.SyncFile{File: target, Size: uint64(manifest.Size())}

	if err := copyondemand.New(deviceName, source, backingFile, false); err != nil {
		return err
	}

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
		manifest:    manifest,
		store:       store,
		chunks:      manifest.Chunks(), // cache !
		cache:       cache,
		cacheExpiry: &sync.Map{},
		sliceCount:  sliceCount,
		buffer:      make([]byte, manifest.chunkMaxSize),
	}
}

func (d *manifestImage) ReadAt(b []byte, off int64) (n int, err error) {
	timeStart := time.Now()

	diskfrom := off
	diskto := diskfrom + int64(len(b))

	// Get slices that contain the data we need to read
	slices, err := d.manifest.SlicesBetween(diskfrom, diskto)
	if err != nil {
		return 0, err
	}

	reads := 0
	errChan := make(chan error)

	diskoff := diskfrom
	bfrom := int64(0)
	for _, slice := range slices {
		chunkoff := diskoff - slice.diskfrom
		blen := minInt64(slice.diskto, diskto) - diskoff
		bto := bfrom + blen

		if slice.kind != kindSparse {
			reads++

			go func(slice *chunkSlice, bfrom int64, bto int64, chunkoff int64) {
				errChan <- d.readSlice(slice, b[bfrom:bto], chunkoff)
			}(slice, bfrom, bto, chunkoff)
		}

		diskoff += blen
		bfrom += blen
	}

	err = nil
	for i := 0; i < reads; i++ {
		if err == nil {
			err = <- errChan
		} else {
			<- errChan
		}
	}

	if err != nil {
		return 0, err
	}

	debugf("READ offset %d, len %d, took %s\n", off, len(b), time.Now().Sub(timeStart))
	return len(b), nil
}

func (d *manifestImage) readSlice(slice *chunkSlice, b []byte, off int64) error {
	timeReadStart := time.Now()
	read, err := d.store.ReadAt(slice.checksum, b, off)
	if err != nil {
		return err
	} else if read != len(b) {
		return errors.New(fmt.Sprintf("cannot read required chunk %x, offset %d, length %d, only read %d bytes",
			slice.checksum, off, len(b), read))
	}
	debugf("Reading chunk %x, offset %d, length %d, took %s",
		slice.checksum, off, len(b), time.Now().Sub(timeReadStart))
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

