package fsdup

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/datto/copyondemand"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type manifestImage struct {
	manifest    *manifest
	store       ChunkStore
	cache       ChunkStore
	cacheQueue  chan *chunkRequest
	chunks      map[string]*chunk
	sliceCount  map[string]int64
	bufferPool  *sync.Pool
}

func Map(manifestId string, store ChunkStore, metaStore MetaStore, cache ChunkStore, targetFile string, fingerprintFileName string) error {
	manifest, err := metaStore.ReadManifest(manifestId)
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

	var fingerprintFile *os.File
	if fingerprintFileName != "" {
		fingerprintFile, err = os.OpenFile(fingerprintFileName, os.O_RDONLY, 0600)
		if err != nil {
			return err
		}
	}


	deviceName, err := findNextNbdDevice()
	if err != nil {
		return err
	}

	debugf("Creating device %s ...\n", deviceName)

	image := NewManifestImage(manifest, store, cache, fingerprintFile)

	source := &copyondemand.SyncSource{Reader: image, Size: uint64(manifest.Size())}
	fs := &copyondemand.LocalFs{}

	if err := copyondemand.New(deviceName, source, targetFile, true); err != nil {
		return err
	}

	return nil
}

type chunkRequest struct {
	checksum []byte
	length int64
}

func NewManifestImage(manifest *manifest, store ChunkStore, cache ChunkStore, fingerprintFile *os.File) *manifestImage {
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

	bufferPool := &sync.Pool{
		New: func() interface{} {
			return make([]byte, manifest.chunkMaxSize)
		},
	}

	cacheChan := make(chan *chunkRequest)

	image := &manifestImage{
		manifest:   manifest,
		store:      store,
		chunks:     manifest.Chunks(), // cache !
		cache:      cache,
		cacheQueue: cacheChan,
		sliceCount: sliceCount,
		bufferPool: bufferPool,
	}

	go func() {
		if fingerprintFile == nil {
			return
		}

		scanner := bufio.NewScanner(fingerprintFile)
		for scanner.Scan() {
			parts := strings.Split(strings.TrimSpace(scanner.Text()), " ")
			if len(parts) != 2 {
				continue
			}

			checksum, err := hex.DecodeString(parts[0])
			if err != nil {
				continue
			}

			length, err := strconv.Atoi(parts[1])
			if err != nil {
				continue
			}

			debugf("Adding fingerprint request to read queue for %x %d\n", checksum, length)
			cacheChan <- &chunkRequest{checksum: checksum, length: int64(length)}
		}
	}()

	for i := 0; i < 200; i++ {
		go func() {
			for request := range cacheChan {
				if err := cache.Stat(request.checksum); err != nil {
					debugf("Reading full chunk %x %d to cache\n", request.checksum, request.length)
					buffer := bufferPool.Get().([]byte)
					defer bufferPool.Put(buffer)

					err = image.readSlice(request.checksum, buffer[:request.length], 0)
					if err != nil {
						return
					}

					cache.Write(request.checksum, buffer[:request.length])
				}
			}
		}()
	}

	return image
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

	// Request full chunks to be cached
	for _, slice := range slices {
		if slice.kind != kindSparse {
			d.cacheQueue <- &chunkRequest{checksum: slice.checksum, length: slice.length}
		}
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
				_, err := d.cache.ReadAt(slice.checksum, b[bfrom:bto], chunkoff)
				if err != nil { // FIXME check length
					errChan <- d.readSlice(slice.checksum, b[bfrom:bto], chunkoff)
				} else {
					errChan <- nil
				}
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

func (d *manifestImage) readSlice(checksum []byte, b []byte, off int64) error {
	timeReadStart := time.Now()
	read, err := d.store.ReadAt(checksum, b, off)
	if err != nil {
		return err
	} else if read != len(b) {
		return errors.New(fmt.Sprintf("cannot read required chunk %x, offset %d, length %d, only read %d bytes",
			checksum, off, len(b), read))
	}
	debugf("Reading chunk %x, offset %d, length %d, took %s",
		checksum, off, len(b), time.Now().Sub(timeReadStart))
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
