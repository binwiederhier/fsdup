package main

import (
	"fmt"
	"heckel.io/fsdup/internal"
)

func printManifestFile(manifestFile string) error {
	manifest, err := readManifestFromFile(manifestFile)
	if err != nil {
		return err
	}

	printManifest(manifest)
	return nil
}

func printManifest(manifest *internal.ManifestV1) {
	offset := int64(0)

	for _, slice := range manifest.Slices {
		if slice.Checksum == nil {
			fmt.Printf("diskoff %013d - %013d len %-10d -> sparse\n",
				offset, offset + slice.Length, slice.Length)
		} else {
			fmt.Printf("diskoff %013d - %013d len %-10d -> chunk %64x chunkoff %10d - %10d\n",
				offset, offset + slice.Length, slice.Length, slice.Checksum, slice.Offset, slice.Offset + slice.Length)
		}

		offset += slice.Length
	}
}

func printManifestStats(manifestFiles []string) error {
	totalImageSize := int64(0)
	totalChunkSize := int64(0)

	chunkMap := make(map[string]int64, 0)

	for _, manifestFile := range manifestFiles {
		manifest, err := readManifestFromFile(manifestFile)
		if err != nil {
			return err
		}

		totalImageSize += manifest.Size

		for _, slice := range manifest.Slices {
			checksumStr := fmt.Sprintf("%x", slice.Checksum)

			// Ignore sparse sections
			if slice.Checksum == nil {
				continue
			}

			// This is a weird way to get the chunk size, but hey ...
			if _, ok := chunkMap[checksumStr]; !ok {
				chunkMap[checksumStr] = slice.Offset + slice.Length
			} else {
				chunkMap[checksumStr] = maxInt64(chunkMap[checksumStr], slice.Offset + slice.Length)
			}
		}
	}

	for _, chunkSize := range chunkMap {
		totalChunkSize += chunkSize
	}

	fmt.Printf("manifests: %d\n", len(manifestFiles))
	fmt.Printf("number of unique chunks: %d\n", len(chunkMap))
	fmt.Printf("total image size: %d\n", totalImageSize)
	fmt.Printf("total chunk size: %d\n", totalChunkSize)

	return nil
}
