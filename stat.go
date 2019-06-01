package fsdup

import (
	"fmt"
	"math"
	"sort"
)

func Stat(manifestFiles []string) error {
	totalImageSize := int64(0)
	totalChunkSize := int64(0)
	totalSparseSize := int64(0)
	usedSizes := make([]int64, 0)

	chunkMap := make(map[string]int64, 0)
	chunkSizes := make([]int64, 0)

	for _, manifestFile := range manifestFiles {
		manifest, err := NewManifestFromFile(manifestFile)
		if err != nil {
			return err
		}

		usedSize := int64(0)
		totalImageSize += manifest.Size()

		for _, breakpoint := range manifest.Breakpoints() {
			part := manifest.Get(breakpoint)

			// Ignore sparse sections
			if part.checksum == nil {
				totalSparseSize += part.to - part.from
				continue
			}

			usedSize += part.to - part.from

			// This is a weird way to get the chunk size, but hey ...
			checksumStr := fmt.Sprintf("%x", part.checksum)

			if _, ok := chunkMap[checksumStr]; !ok {
				chunkMap[checksumStr] = part.to
			} else {
				chunkMap[checksumStr] = maxInt64(chunkMap[checksumStr], part.to)
			}
		}

		usedSizes = append(usedSizes, usedSize)
	}

	for _, chunkSize := range chunkMap {
		totalChunkSize += chunkSize
		chunkSizes = append(chunkSizes, chunkSize)
	}

	sort.Slice(chunkSizes, func(i, j int) bool {
		return chunkSizes[i] < chunkSizes[j]
	})

	medianChunkSize := int64(0)
	if len(chunkSizes) % 2 == 0 {
		medianChunkSize = chunkSizes[len(chunkSizes)/2]
	} else {
		medianChunkSize = chunkSizes[(len(chunkSizes)-1)/2]
	}

	manifestCount := int64(len(manifestFiles))
	chunkCount := int64(len(chunkMap))
	averageChunkSize := int64(math.Round(float64(totalChunkSize) / float64(chunkCount)))

	totalUsedSize := totalImageSize - totalSparseSize
	dedupRatio := float64(totalUsedSize) / float64(totalChunkSize) // as x:1 ratio
	spaceReductionRatio := (1 - 1/dedupRatio) * 100                // in %

	fmt.Printf("Manifests:               %d\n", manifestCount)
	fmt.Printf("Number of unique chunks: %d\n", chunkCount)
	fmt.Printf("Total image size:        %s (%d bytes)\n", convertToHumanReadable(totalImageSize), totalImageSize)
	fmt.Printf("Total used size:         %s (%d bytes)\n", convertToHumanReadable(totalUsedSize), totalUsedSize)
	fmt.Printf("Total sparse size:       %s (%d bytes)\n", convertToHumanReadable(totalSparseSize), totalSparseSize)
	fmt.Printf("Total chunk size:        %s (%d bytes)\n", convertToHumanReadable(totalChunkSize), totalChunkSize)
	fmt.Printf("Average chunk size:      %s (%d bytes)\n", convertToHumanReadable(averageChunkSize), averageChunkSize)
	fmt.Printf("Median chunk size:       %s (%d bytes)\n", convertToHumanReadable(medianChunkSize), medianChunkSize)
	fmt.Printf("Dedup ratio:             %.1f : 1\n", dedupRatio)
	fmt.Printf("Space reduction ratio:   %.1f %%\n", spaceReductionRatio)

	return nil
}
