package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/samalba/buse-go/buse"
	"heckel.io/fsdup/internal"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
)

type localManifestImage struct {
	manifest *internal.ManifestV1
}

func (d *localManifestImage) ReadAt(p []byte, off uint) error {
	log.Printf("[localManifestImage] READ offset:%d len:%d\n", off, len(p))

	// Find from/to chunk
	// FIXME this is inefficient

	chunkOffset := int64(0)
	fromChunkIndex := -1
	toChunkIndex := -1
	for i, slice := range d.manifest.Slices {
		if fromChunkIndex == -1 {
			if int64(off) >= chunkOffset {
				fromChunkIndex = i
			}
		}

		if toChunkIndex == -1 {
			if int64(off) <= chunkOffset {
				toChunkIndex = i
				break
			}
		}

		chunkOffset += slice.Length
	}

	fromOffset := int64(off)
	toOffset := fromOffset + int64(len(p)) - 1

	log.Printf("[localManifestImage] - fromOffset = %d, to = %d, fromChunk = %d, toChunk = %d\n", fromOffset, toOffset,
		fromChunkIndex, toChunkIndex)

	pFrom := int64(0)
	chunkFrom := int64(0)
	chunkTo := int64(0)

	for chunkIndex := fromChunkIndex; chunkIndex <= toChunkIndex; chunkIndex++ {
		chunkLength := d.manifest.Slices[chunkIndex].Length
		chunkChecksum := d.manifest.Slices[chunkIndex].Checksum

		if chunkIndex == fromChunkIndex {
			chunkFrom = fromOffset % chunkLength
		} else {
			chunkFrom = 0
		}

		if chunkIndex == toChunkIndex {
			chunkTo = toOffset % chunkLength
		} else {
			chunkTo = chunkLength - 1
		}

		chunkPartLen := chunkTo - chunkFrom + 1
		pTo := pFrom + chunkPartLen

		log.Printf("[localManifestImage] - idx = %d, checksum = %x, chunkFrom = %d, chunkTo = %d, chunkPartLen = %d, pFrom = %d, pTo = %d\n",
			chunkIndex, chunkChecksum, chunkFrom, chunkTo, chunkPartLen, pFrom, pTo)

		chunkFile := fmt.Sprintf("index/%x", chunkChecksum)

		f, err := os.OpenFile(chunkFile, os.O_RDONLY, 0666)
		if err != nil {
			panic(err)
		}

		read, err := f.ReadAt(p[pFrom:pTo], chunkFrom)
		if err != nil {
			panic(err)
		}
		if int64(read) != chunkPartLen {
			panic(fmt.Sprintf("invalid len read. expected %d, but read %d", chunkPartLen, read))
		}

		pFrom += int64(read)
	}

	log.Printf("[localManifestImage] READ offset:%d len:%d\n", off, len(p))
	return nil
}

func (d *localManifestImage) WriteAt(p []byte, off uint) error {
	//d.file.WriteAt(p, int64(off))
	// TODO NOP
	log.Printf("[localManifestImage] WRITE offset:%d len:%d\n", off, len(p))
	return nil
}

func (d *localManifestImage) Disconnect() {
	log.Println("[localManifestImage] DISCONNECT")
}

func (d *localManifestImage) Flush() error {
	log.Println("[localManifestImage] FLUSH")
	return nil
}

func (d *localManifestImage) Trim(off, length uint) error {
	log.Printf("[localManifestImage] TRIM offset:%d len:%d\n", off, length)
	return nil
}

func mountManifest(manifestFile string) {
	in, err := ioutil.ReadFile(manifestFile)
	if err != nil {
		log.Fatalln("Error reading file:", err)
	}
	manifest := &internal.ManifestV1{}
	if err := proto.Unmarshal(in, manifest); err != nil {
		log.Fatalln("Failed to parse address book:", err)
	}

	localManifestImage := &localManifestImage{
		manifest: manifest,
	}

	device, err := buse.CreateDevice("/dev/nbd0", uint(manifest.Size), localManifestImage)
	if err != nil {
		fmt.Printf("Cannot create device: %s\n", err)
		os.Exit(1)
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
	fmt.Println("SIGINT, disconnecting...")
	device.Disconnect()
}
