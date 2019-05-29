package main

import (
	"fmt"
	"math"
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
	totalSparseSize := int64(0)

	chunkMap := make(map[string]int64, 0)

	for _, manifestFile := range manifestFiles {
		manifest, err := NewManifestFromFile(manifestFile)
		if err != nil {
			return err
		}

		totalImageSize += manifest.Size()

		for _, breakpoint := range manifest.Breakpoints() {
			part := manifest.Get(breakpoint)

			// Ignore sparse sections
			if part.checksum == nil {
				totalSparseSize += part.to - part.from
				continue
			}

			// This is a weird way to get the chunk size, but hey ...
			checksumStr := fmt.Sprintf("%x", part.checksum)

			if _, ok := chunkMap[checksumStr]; !ok {
				chunkMap[checksumStr] = part.to
			} else {
				chunkMap[checksumStr] = maxInt64(chunkMap[checksumStr], part.to)
			}
		}
	}

	for _, chunkSize := range chunkMap {
		totalChunkSize += chunkSize
	}

	manifestCount := int64(len(manifestFiles))
	chunkCount := int64(len(chunkMap))
	averageChunkSize := int64(math.Round(float64(totalChunkSize) / float64(chunkCount)))

	totalOnDiskSize := totalImageSize - totalSparseSize
	dedupRatio := float64(totalOnDiskSize) / float64(totalChunkSize) // as x:1 ratio
	spaceReductionRatio := (1 - 1/dedupRatio) * 100 // in %

	fmt.Printf("Manifests:               %d\n", manifestCount)
	fmt.Printf("Number of unique chunks: %d\n", chunkCount)
	fmt.Printf("Total image size:        %s (%d bytes)\n", convertToHumanReadable(totalImageSize), totalImageSize)
	fmt.Printf("Total on disk size:      %s (%d bytes)\n", convertToHumanReadable(totalOnDiskSize), totalOnDiskSize)
	fmt.Printf("Total sparse size:       %s (%d bytes)\n", convertToHumanReadable(totalSparseSize), totalSparseSize)
	fmt.Printf("Total chunk size:        %s (%d bytes)\n", convertToHumanReadable(totalChunkSize), totalChunkSize)
	fmt.Printf("Average chunk size:      %s (%d bytes)\n", convertToHumanReadable(averageChunkSize), averageChunkSize)
	fmt.Printf("Dedup ratio:             %.1f : 1\n", dedupRatio)
	fmt.Printf("Space reduction ratio:   %.1f %%\n", spaceReductionRatio)

	return nil
}
