package fsdup

import (
	"errors"
	"fmt"
	"github.com/samalba/buse-go/buse"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
)

type manifestImage struct {
	manifest    *manifest
	store       ChunkStore
	breakpoints []int64
}

func NewManifestImage(manifest *manifest, store ChunkStore) *manifestImage {
	return &manifestImage{
		manifest: manifest,
		store: store,
		breakpoints: manifest.Breakpoints(), // cache !
	}
}

func (d *manifestImage) ReadAt(p []byte, off uint) error {
	log.Printf("\nREAD offset:%d len:%d\n", off, len(p))

	// Find first chunk
	// FIXME Linear search is inefficient; use binary search instead, or something better.
	breakpointIndex := 0
	requestedStart := int64(off)

	for i := 0; i < len(d.breakpoints); i++ {
		part := d.manifest.Get(d.breakpoints[i])
		partStart := d.breakpoints[i]
		partEnd := partStart + part.to - part.from

		if partStart <= requestedStart && requestedStart < partEnd {
			breakpointIndex = i
			Debugf("breakpoint index = %d\n", breakpointIndex)
			break
		}
	}

	// Read chunk
	bufferOffset := int64(0)
	currentOffset := int64(off)
	remainingToRead := int64(len(p))

	for remainingToRead > 0 {
		part := d.manifest.Get(d.breakpoints[breakpointIndex])
		partStart := d.breakpoints[breakpointIndex]
		partEnd := partStart + part.to - part.from
		partOffset := part.from + currentOffset - partStart
		maxChunkBytes := minInt64(part.to - part.from, partEnd - currentOffset)
		bytesToRead := minInt64(maxChunkBytes, remainingToRead)

		var read int
		var err error

		if part.checksum == nil {
			log.Printf("- Reading disk offset %d - %d as sparse chunk (len %d)\n",
				currentOffset, currentOffset + bytesToRead, bytesToRead)

			// Note: This assumes that the buffer "p" is empty!
			// Let's hope that is true.

			read = int(bytesToRead)
		} else {
			log.Printf("- Reading disk offset %d - %d to buffer %d - %d from chunk %x, offset %d - %d (len %d)\n",
				currentOffset, currentOffset + bytesToRead, bufferOffset, bufferOffset + bytesToRead,
				part.checksum, partOffset, partOffset + bytesToRead, bytesToRead)

			read, err = d.store.ReadAt(part.checksum, p[bufferOffset:bufferOffset+bytesToRead], partOffset)
			if err != nil {
				return err
			}
		}

		currentOffset += int64(read)
		bufferOffset += int64(read)
		remainingToRead -= int64(read)
		breakpointIndex++
	}

	return nil
}

func (d *manifestImage) WriteAt(p []byte, off uint) error {
	//d.file.WriteAt(p, int64(off))
	// TODO NOP
	log.Printf("[localManifestImage] WRITE offset:%d len:%d\n", off, len(p))
	return nil
}

func (d *manifestImage) Disconnect() {
	log.Println("[localManifestImage] DISCONNECT")
}

func (d *manifestImage) Flush() error {
	log.Println("[localManifestImage] FLUSH")
	return nil
}

func (d *manifestImage) Trim(off, length uint) error {
	log.Printf("[localManifestImage] TRIM offset:%d len:%d\n", off, length)
	return nil
}

func Map(manifestFile string, store ChunkStore) error {
	manifest, err := NewManifestFromFile(manifestFile)
	if err != nil {
		return err
	}

	deviceName, err := findNbdDevice()
	if err != nil {
		return err
	}

	Debugf("Creating device %s ...\n", deviceName)

	image := NewManifestImage(manifest, store)
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
	Debugf("SIGINT, disconnecting...\n")
	device.Disconnect()

	return nil
}

func findNbdDevice() (string, error) {
	for i := 0; i < 32; i++ {
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