package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"golang.org/x/crypto/blake2b"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/samalba/buse-go/buse"
)

const chunkSize = 4 * 1024 * 1024

type radosImage struct {
	pool *rados.IOContext
	checksums []string
}

func (d *radosImage) ReadAt(p []byte, off uint) error {
	log.Printf("[radosImage] READ offset:%d len:%d\n", off, len(p))

	fromOffset := int(off)
	toOffset := fromOffset + len(p) - 1
	fromChunkIndex := int(math.Floor(float64(fromOffset) / chunkSize))
	toChunkIndex := int(math.Floor(float64(toOffset) / chunkSize))

	log.Printf("[radosImage] - fromOffset = %d, to = %d, fromChunk = %d, toChunk = %d\n", fromOffset, toOffset,
		fromChunkIndex, toChunkIndex)

	pFrom := 0
	chunkFrom := 0
	chunkTo := 0

	for chunkIndex := fromChunkIndex; chunkIndex <= toChunkIndex; chunkIndex++ {
		chunkChecksum := d.checksums[chunkIndex]

		if chunkIndex == fromChunkIndex {
			chunkFrom = fromOffset % chunkSize
		} else {
			chunkFrom = 0
		}

		if chunkIndex == toChunkIndex {
			chunkTo = toOffset % chunkSize
		} else {
			chunkTo = chunkSize - 1
		}

		chunkPartLen := chunkTo - chunkFrom + 1
		pTo := pFrom + chunkPartLen

		log.Printf("[radosImage] - idx = %d, checksum = %s, chunkFrom = %d, chunkTo = %d, chunkPartLen = %d, pFrom = %d, pTo = %d\n",
			chunkIndex, chunkChecksum, chunkFrom, chunkTo, chunkPartLen, pFrom, pTo)

		read, err := d.pool.Read(chunkChecksum, p[pFrom:pTo], uint64(chunkFrom))
		if err != nil {
			panic(err)
		}
		if read != chunkPartLen {
			panic(fmt.Sprintf("invalid len read. expected %d, but read %d", chunkPartLen, read))
		}

		pFrom += read
	}

	log.Printf("[radosImage] READ offset:%d len:%d\n", off, len(p))
	return nil
}

func (d *radosImage) WriteAt(p []byte, off uint) error {
	//d.file.WriteAt(p, int64(off))
	// TODO NOP
	log.Printf("[radosImage] WRITE offset:%d len:%d\n", off, len(p))
	return nil
}

func (d *radosImage) Disconnect() {
	log.Println("[radosImage] DISCONNECT")
}

func (d *radosImage) Flush() error {
	log.Println("[radosImage] FLUSH")
	return nil
}

func (d *radosImage) Trim(off, length uint) error {
	log.Printf("[radosImage] TRIM offset:%d len:%d\n", off, length)
	return nil
}

func openPool() *rados.IOContext {
	conn, err := rados.NewConn()
	if err != nil {
		panic(err)
	}

	if err := conn.ReadConfigFile("ceph.conf"); err != nil {
		panic(err)
	}

	if err := conn.Connect(); err != nil {
		panic(err)
	}

	pool, err := conn.OpenIOContext("phil1")
	if err != nil {
		panic(err)
	}

	return pool
}

// index reads a file and slices it into pieces, and writes the slices
// to ceph. It also writes a metadata file "id" to ceph containing a reference
// to all the slices.
func index(id string, filename string) {
	file, err := os.Open(filename)
	check(err, 1, "cannot open file " + filename)

	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}

	pool := openPool()

	metadata := make([]string, 0)
	buffer := make([]byte, chunkSize)
	reader := bufio.NewReader(file)

	metadata = append(metadata, "1") // version
	metadata = append(metadata, strconv.Itoa(int(stat.Size()))) // size

	num := 0

	for {
		read, err := reader.Read(buffer)

		if err == io.EOF {
			break
		} else if err != nil {
			exit(2, "Cannot read file " + filename)
		}

		checksumBytes := blake2b.Sum256(buffer[:read])
		checksum := hex.EncodeToString(checksumBytes[:])

		_, err = pool.Stat(checksum)
		if err != nil {
			if err := pool.WriteFull(checksum, buffer[:read]); err != nil {
				panic(err)
			}

			fmt.Printf("chunkSlice is NEW %s\n", checksum)
		} else {
			fmt.Printf("chunkSlice exists %s\n", checksum)
		}

		metadata = append(metadata, checksum)
		num++
	}

	if err := pool.WriteFull(id, []byte(strings.Join(metadata,"\n"))); err != nil { // stupid
		panic(err)
	}

	println(id + " indexed")
}

// mount creates a block device that is backed by the slices in
// ceph. It uses the metadata file written by the index function.
func mount(id string) {
	pool := openPool()

	stat, err := pool.Stat(id)
	if err != nil {
		panic(err)
	}

	metadataBytes := make([]byte, stat.Size)
	_, err = pool.Read(id, metadataBytes, 0)
	if err != nil {
		panic(err)
	}

	metadata := strings.Split(string(metadataBytes), "\n")

	if len(metadata) < 3 || metadata[0] != "1" {
		panic("invalid metadata: " + string(metadataBytes))
	}

	size, err := strconv.Atoi(metadata[1])
	if err != nil {
		panic(err)
	}

	checksums := metadata[2:]

	deviceExp := &radosImage{
		pool: pool,
		checksums: checksums,
	}

	device, err := buse.CreateDevice("/dev/nbd0", uint(size), deviceExp)
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

func check(err error, code int, message string) {
	if err != nil {
		exit(code, message)
	}
}

func exit(code int, message string) {
	fmt.Println(message)
	os.Exit(code)
}

func main() {
	indexCommand := flag.NewFlagSet("index", flag.ExitOnError)
	mountCommand := flag.NewFlagSet("mount", flag.ExitOnError)

	if len(os.Args) < 2 {
		exit(1, "Syntax: cephboot index ID FILE\n        cephboot mount ID")
	}

	command := os.Args[1]

	switch command {
	case "index":
		indexCommand.Parse(os.Args[2:])

		if len(os.Args) < 4 {
			exit(1, "Syntax: cephboot index ID FILE")
		}

		id := os.Args[2]
		file := os.Args[3]

		index(id, file)
	case "parsentfs":
		indexCommand.Parse(os.Args[2:])

		if len(os.Args) < 2 {
			exit(1, "Syntax: cephboot parsentfs FILE")
		}

		file := os.Args[2]

		parseNTFS(file)
	case "mount":
		mountCommand.Parse(os.Args[2:])

		if len(os.Args) < 4 {
			exit(1, "Syntax: cephboot mount ID")
		}

		id := os.Args[2]

		mount(id)
	default:
		flag.PrintDefaults()
		os.Exit(1)
	}
}
func read_int32(data []byte) (ret int32) {
	buf := bytes.NewBuffer(data)
	binary.Read(buf, binary.LittleEndian, &ret)
	return
}
