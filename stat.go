package fsdup

import (
	"fmt"
	"math"
	"sort"
)

type chunkStat struct {
	checksum   []byte
	size       int64
	sliceCount int64
	sliceSizes int64
	kind       kind
}

func Stat(manifestIds []string, metaStore MetaStore, verbose bool) error {
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
	chunkStats := make([]*chunkStat, 0)

	for i, manifestId := range manifestIds {
		statusf("Reading manifest %d/%d ...", i, len(manifestIds))
		debugf("Reading manifest file %s ...\n", manifestId)

		manifest, err := metaStore.ReadManifest(manifestId)
		if err != nil {
			return err
		}

		usedSize := int64(0)
		totalImageSize += manifest.Size()

		for _, offset := range manifest.Offsets() {
			slice := manifest.Get(offset)

			// Ignore sparse sections
			if slice.checksum == nil {
				totalSparseSize += slice.chunkto - slice.chunkfrom
				continue
			}

			sliceSize := slice.chunkto - slice.chunkfrom
			usedSize += sliceSize

			if slice.kind == kindFile {
				totalFileSize += sliceSize
			} else if slice.kind == kindGap {
				totalGapSize += sliceSize
			} else {
				totalUnknownSize += sliceSize
			}

			// This is a weird way to get the chunk size, but hey ...
			checksumStr := fmt.Sprintf("%x", slice.checksum)

			if _, ok := chunkMap[checksumStr]; !ok {
				chunkMap[checksumStr] = &chunkStat{
					checksum:   slice.checksum,
					size:       slice.chunkto,
					sliceCount: 1,
					sliceSizes: sliceSize,
					kind:       slice.kind, // This is inaccurate, because only the first appearance of the chunk is counted!
				}
			} else {
				chunkMap[checksumStr].size = maxInt64(chunkMap[checksumStr].size, slice.chunkto)
				chunkMap[checksumStr].sliceCount++
				chunkMap[checksumStr].sliceSizes += sliceSize
			}
		}
	}

	statusf("Crunching numbers ...")

	// Find chunk sizes by type
	for _, stat := range chunkMap {
		totalChunkSize += stat.size
		chunkSizes = append(chunkSizes, stat.size)
		chunkStats = append(chunkStats, stat)

		if stat.kind == kindFile {
			totalFileChunkSize += stat.size
		} else if stat.kind == kindGap {
			totalGapChunkSize += stat.size
		} else {
			totalUnknownChunkSize += stat.size
		}
	}

	// Find median chunk size
	sort.Slice(chunkSizes, func(i, j int) bool {
		return chunkSizes[i] < chunkSizes[j]
	})

	medianChunkSize := int64(0)
	if len(chunkSizes) % 2 == 0 {
		medianChunkSize = chunkSizes[len(chunkSizes)/2]
	} else {
		medianChunkSize = chunkSizes[(len(chunkSizes)-1)/2]
	}

	// Find chunk histogram
	sort.Slice(chunkStats, func(i, j int) bool {
		return chunkStats[i].sliceSizes > chunkStats[j].sliceSizes
	})

	manifestCount := int64(len(manifestIds))
	chunkCount := int64(len(chunkMap))
	averageChunkSize := int64(math.Round(float64(totalChunkSize) / float64(chunkCount)))

	totalUsedSize := totalImageSize - totalSparseSize
	dedupRatio := float64(totalUsedSize) / float64(totalChunkSize) // as x:1 ratio
	spaceReductionPercentage := (1 - 1/dedupRatio) * 100           // in %

	statusf("")

	fmt.Printf("Manifests:                  %d\n", manifestCount)
	fmt.Printf("Number of unique chunks:    %d\n", chunkCount)
	fmt.Printf("Total image size:           %s (%d bytes)\n", convertBytesToHumanReadable(totalImageSize), totalImageSize)
	fmt.Printf("- Used:                     %s (%d bytes)\n", convertBytesToHumanReadable(totalUsedSize), totalUsedSize)
	fmt.Printf("  - Files:                  %s (%d bytes)\n", convertBytesToHumanReadable(totalFileSize), totalFileSize)
	fmt.Printf("  - Gaps:                   %s (%d bytes)\n", convertBytesToHumanReadable(totalGapSize), totalGapSize)
	fmt.Printf("  - Unknown:                %s (%d bytes)\n", convertBytesToHumanReadable(totalUnknownSize), totalUnknownSize)
	fmt.Printf("- Sparse/empty:             %s (%d bytes)\n", convertBytesToHumanReadable(totalSparseSize), totalSparseSize)
	fmt.Printf("Total chunk size:           %s (%d bytes)\n", convertBytesToHumanReadable(totalChunkSize), totalChunkSize)
	fmt.Printf("- File chunks:              %s (%d bytes)\n", convertBytesToHumanReadable(totalFileChunkSize), totalFileChunkSize)
	fmt.Printf("- Gap chunks:               %s (%d bytes)\n", convertBytesToHumanReadable(totalGapChunkSize), totalGapChunkSize)
	fmt.Printf("- Unkown chunks:            %s (%d bytes)\n", convertBytesToHumanReadable(totalUnknownChunkSize), totalUnknownChunkSize)
	fmt.Printf("Average chunk size:         %s (%d bytes)\n", convertBytesToHumanReadable(averageChunkSize), averageChunkSize)
	fmt.Printf("Median chunk size:          %s (%d bytes)\n", convertBytesToHumanReadable(medianChunkSize), medianChunkSize)
	fmt.Printf("Dedup ratio:                %.1f : 1\n", dedupRatio)
	fmt.Printf("Space reduction:            %.1f %%\n", spaceReductionPercentage)

	if verbose {
		fmt.Printf("Slice histogram (top 10):\n")
		for i, stat := range chunkStats {
			fmt.Printf("- Chunk %x: %s in %d slice(s)\n", stat.checksum, convertBytesToHumanReadable(stat.sliceSizes), stat.sliceCount)
			if i == 10 {
				break
			}
		}
	}

	return nil
}
