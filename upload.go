package fsdup

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"heckel.io/fsdup/pb"
	"os"
)

func Upload(manifestFile string, inputFile string, serverAddr string) error {
	manifest, err := NewManifestFromFile(manifestFile)
	if err != nil {
		return err
	}

	// Get list of all chunks
	checksums := make([][]byte, 0)
	for checksumStr, _ := range manifest.Chunks() {
		checksum, err := hex.DecodeString(checksumStr)
		if err != nil {
			return err
		}

		if len(checksum) > 0 {
			checksums = append(checksums, checksum)
		}
	}

	// Find unknown chunks
	conn, err := grpc.Dial(serverAddr, grpc.WithBlock(), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(128 * 1024 * 1024))) // FIXME
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewHubClient(conn)

	response, err := client.Diff(context.Background(), &pb.DiffRequest{
		Id: "1",
		Checksums: checksums,
	}, )

	if err != nil {
		return err
	}

	unknowns := make(map[string]bool, 0)
	for _, checksum := range response.UnknownChecksums {
		unknowns[fmt.Sprintf("%x", checksum)] = true
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

	buffer := make([]byte, manifest.chunkMaxSize)

	for _, checksumStr := range manifest.ChecksumsByDiskOffset(chunkSlices) {
		if _, ok := unknowns[checksumStr]; !ok {
			continue
		}

		slices := chunkSlices[checksumStr]

		checksum, err := hex.DecodeString(checksumStr) // FIXME this is ugly. checksum should be its own type.
		if err != nil {
			return err
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

		debugf("uploading %x\n", checksum)

		_, err = client.PutChunk(context.Background(), &pb.PutChunkRequest{
			Id: "1",
			Checksum: checksum,
			Data: buffer[:chunkSize],
		})

		if err != nil {
			return err
		}
	}

	client.PutManifest(context.Background(), &pb.PutManifestRequest{
		Id: "1",
		Manifest: manifest.Proto(),
	})

	return nil
}
