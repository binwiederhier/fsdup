package fsdup

import (
	"fmt"
	"math"
	"sort"
)

type chunkStat struct {
	size int64
	kind kind
}

func Stat(manifestFiles []string) error {
	totalImageSize := int64(0)
	totalFileSize := int64(0)
	totalGapSize := int64(0)
	totalUnknownSize := int64(0)

	totalSparseSize := int64(0)

	totalChunkSize := int64(0)
	totalFileChunkSize := int64(0)
	totalGapChunkSize := int64(0)
	totalUnknownChunkSize := int64(0)

	chunkMap := make(map[string]*chunkStat, 0) // checksum -> size
	chunkSizes := make([]int64, 0)

	for _, manifestFile := range manifestFiles {
		manifest, err := NewManifestFromFile(manifestFile)
		if err != nil {
			return err
		}

		usedSize := int64(0)
		totalImageSize += manifest.Size()

		for _, breakpoint := range manifest.Offsets() {
			part := manifest.Get(breakpoint)

			// Ignore sparse sections
			if part.checksum == nil {
				totalSparseSize += part.to - part.from
				continue
			}

			partSize := part.to - part.from
			usedSize += partSize

			if part.kind == kindFile {
				totalFileSize += partSize
			} else if part.kind == kindGap {
				totalGapSize += partSize
			} else {
				totalUnknownSize += partSize
			}

			// This is a weird way to get the chunk size, but hey ...
			checksumStr := fmt.Sprintf("%x", part.checksum)

			if _, ok := chunkMap[checksumStr]; !ok {
				chunkMap[checksumStr] = &chunkStat{
					size: part.to,
					kind: part.kind, // This is inaccurate, because only the first appearance of the chunk is counted!
				}
			} else {
				chunkMap[checksumStr].size = maxInt64(chunkMap[checksumStr].size, part.to)
			}
		}
	}

	for _, stat := range chunkMap {
		totalChunkSize += stat.size
		chunkSizes = append(chunkSizes, stat.size)

		if stat.kind == kindFile {
			totalFileChunkSize += stat.size
		} else if stat.kind == kindGap {
			totalGapChunkSize += stat.size
		} else {
			totalUnknownChunkSize += stat.size
		}
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
	spaceReductionPercentage := (1 - 1/dedupRatio) * 100           // in %

	fmt.Printf("Manifests:                  %d\n", manifestCount)
	fmt.Printf("Number of unique chunks:    %d\n", chunkCount)
	fmt.Printf("Total image size:           %s (%d bytes)\n", convertToHumanReadable(totalImageSize), totalImageSize)
	fmt.Printf("- Used:                     %s (%d bytes)\n", convertToHumanReadable(totalUsedSize), totalUsedSize)
	fmt.Printf("  - Files:                  %s (%d bytes)\n", convertToHumanReadable(totalFileSize), totalFileSize)
	fmt.Printf("  - Gaps:                   %s (%d bytes)\n", convertToHumanReadable(totalGapSize), totalGapSize)
	fmt.Printf("  - Unknown:                %s (%d bytes)\n", convertToHumanReadable(totalUnknownSize), totalUnknownSize)
	fmt.Printf("- Sparse/empty:             %s (%d bytes)\n", convertToHumanReadable(totalSparseSize), totalSparseSize)
	fmt.Printf("Total chunk size:           %s (%d bytes)\n", convertToHumanReadable(totalChunkSize), totalChunkSize)
	fmt.Printf("- File chunks:              %s (%d bytes)\n", convertToHumanReadable(totalFileChunkSize), totalFileChunkSize)
	fmt.Printf("- Gap chunks:               %s (%d bytes)\n", convertToHumanReadable(totalGapChunkSize), totalGapChunkSize)
	fmt.Printf("- Unkown chunks:            %s (%d bytes)\n", convertToHumanReadable(totalUnknownChunkSize), totalUnknownChunkSize)
	fmt.Printf("Average chunk size:         %s (%d bytes)\n", convertToHumanReadable(averageChunkSize), averageChunkSize)
	fmt.Printf("Median chunk size:          %s (%d bytes)\n", convertToHumanReadable(medianChunkSize), medianChunkSize)
	fmt.Printf("Dedup ratio:                %.1f : 1\n", dedupRatio)
	fmt.Printf("Space reduction:            %.1f %%\n", spaceReductionPercentage)

	return nil
}
