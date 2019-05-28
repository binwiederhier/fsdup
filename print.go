package main

import (
	"fmt"
)

func printManifestFile(manifestFile string) error {
	manifest, err := NewManifestFromFile(manifestFile)
	if err != nil {
		return err
	}
	
	manifest.Print()
	return nil
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
