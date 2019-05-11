package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"heckel.io/cephboot/internal"
	"io"
	"math"
	"os"
	"strconv"
)

const (
	NTFS_MAGIC_OFFSET       = 3
	NTFS_MAGIC              = "NTFS    "
	NTFS_SECTOR_SIZE_OFFSET = 11
	NTFS_SECTOR_SIZE_LENGTH = 2
	ENTRY_MAGIC             = "FILE"
	ENTRY_FIRST_ATTR_OFFSET = 20
	ENTRY_FIRST_ATTR_LENTGH = 2
	ATTR_TYPE_DATA          = 0x80
	ATTR_TYPE_END_MARKER    = 0xFFFFFF
)

type run struct {
	sparse bool
	fromOffset   uint64
	toOffset     uint64
	firstCluster int64 // signed!
	clusterCount uint64
}

type entry struct {
	fileSize uint64
	runs     []run
}

var ErrDataResident = errors.New("data is resident")
var ErrNoDataAttr = errors.New("no data attribute")
var ErrUnexpectedMagic = errors.New("unexpected magic")
var ErrFileTooSmallToIndex = errors.New("file too small to index")

func readRuns(reader io.ReaderAt, clusterSize int64, offset int64) []run {
	runs := make([]run, 0)
	firstCluster := int64(0)

	for {
		header := readUintLE(reader, offset, 1)

		if header == 0 {
			break
		}

		clusterCountLength := header & 0x0F
		clusterCountOffset := offset + 1
		clusterCount := readUintLE(reader, clusterCountOffset, int64(clusterCountLength))

		firstClusterLength := header & 0xF0 >> 4
		firstClusterOffset := int64(clusterCountOffset) + int64(clusterCountLength)
		firstCluster += readIntLE(reader, int64(firstClusterOffset), int64(firstClusterLength)) // relative to previous, can be negative, so signed!

		sparse := firstClusterLength == 0

		fromOffset := int64(firstCluster) * int64(clusterSize)
		toOffset := fromOffset + int64(clusterCount) * int64(clusterSize)

		fmt.Printf("data run offset = %d, header = 0x%x, sparse = %b, length length = 0x%x, offset length = 0x%x, " +
			"cluster count = %d, first cluster = %d, from offset = %d, to offset = %d\n",
			offset, header, sparse, clusterCountLength, firstClusterLength, clusterCount, firstCluster,
			fromOffset, toOffset)

		runs = append(runs, run{
			sparse: sparse,
			firstCluster: firstCluster,
			clusterCount: clusterCount,
			fromOffset:   uint64(fromOffset),
			toOffset:     uint64(toOffset),
		})

		offset += int64(firstClusterLength) + int64(clusterCountLength) + 1
	}

	return runs
}

func readEntry(reader io.ReaderAt, clusterSize int64, offset int64) (*entry, error) {
	err := readAndCompare(reader, int64(offset), []byte(ENTRY_MAGIC))
	if err != nil {
		return nil, err
	}

	relativeFirstAttrOffset := readUintLE(reader, offset + ENTRY_FIRST_ATTR_OFFSET, ENTRY_FIRST_ATTR_LENTGH)

	entry := &entry{}

	firstAttrOffset := offset + int64(relativeFirstAttrOffset)
	attrOffset := firstAttrOffset

	for {
		attrType := readUintLE(reader, attrOffset, 4)
		attrLen := readUintLE(reader, attrOffset + 4, 4)

		if attrType == ATTR_TYPE_DATA {
			nonResident := readUintLE(reader, attrOffset + 8, 1)

			if nonResident == 0 {
				return nil, ErrDataResident
			}

			dataRealSize := readUintLE(reader, attrOffset + 48, 8)

/*			if fileSize < 2 * 1024 * 1024 {
				return nil, ErrFileTooSmallToIndex
			}*/

			relativeDataRunsOffset := readUintLE(reader, attrOffset + 32, 2)
			dataRunFirstOffset := attrOffset + int64(relativeDataRunsOffset)

			entry.fileSize = dataRealSize
			entry.runs = readRuns(reader, clusterSize, dataRunFirstOffset)
		} else if attrType == ATTR_TYPE_END_MARKER || attrLen == 0 {
			break
		}

		attrOffset += int64(attrLen)
	}

	if entry.runs == nil {
		return nil, ErrNoDataAttr
	}

	return entry, nil
}

func readAndCompare(reader io.ReaderAt, offset int64, expected []byte) error {
	actual := make([]byte, len(expected))
	n, err := reader.ReadAt(actual, offset)

	if err != nil {
		return err
	} else if n != len(actual) || bytes.Compare(expected, actual) != 0 {
		return ErrUnexpectedMagic
	}

	return nil
}

func parseNTFS(filename string) {
	fs, err := os.Open(filename)
	check(err, 1, "cannot open file "+filename)

	defer fs.Close()

	stat, err := fs.Stat()
	if err != nil {
		panic(err)
	}

	err = readAndCompare(fs, NTFS_MAGIC_OFFSET, []byte(NTFS_MAGIC))
	if err != nil {
		panic(err)
	}

	sectorSize := readUintLE(fs, NTFS_SECTOR_SIZE_OFFSET, NTFS_SECTOR_SIZE_LENGTH)
	sectorsPerCluster := readUintLE(fs, 13, 1)
	clusterSize := sectorSize * sectorsPerCluster
	mftClusterNumber := readUintLE(fs, 48, 8)
	mftOffset := mftClusterNumber * clusterSize

	fmt.Printf("sector size = %d, sectors per cluster = %d, cluster size = %d, mft cluster number = %d, mft offset = %d\n",
		sectorSize, sectorsPerCluster, clusterSize, mftClusterNumber, mftOffset)

	mft, err := readEntry(fs, int64(clusterSize), int64(mftOffset))
	if err != nil {
		panic(err)
	}

	fmt.Printf("\n\nMFT: %#v\n\n", mft)

	for _, run := range mft.runs {
		fmt.Printf("\n\nprocessing run: %#v\n\n", run)
		entries := int(run.clusterCount * sectorsPerCluster)
		firstSector := int64(run.firstCluster) * int64(sectorsPerCluster)

		for i := 1; i < entries; i++ { // Skip entry 0 ($MFT itself!)
			sector := firstSector + int64(i)
			sectorOffset := sector * int64(sectorSize)
			// TODO we should look at entry.allocSize and jump ahead. This will cut the reads in half!

			fmt.Printf("reading entry at sector %d / offset %d:\n", sector, sectorOffset)

			entry, err := readEntry(fs, int64(clusterSize), sectorOffset)
			if err == ErrDataResident || err == ErrNoDataAttr || err == ErrUnexpectedMagic {
				fmt.Printf("skipping FILE: %s\n\n", err.Error())
				continue
			} else if err != nil {
				panic(err)
			}

			fmt.Printf("FILE %#v\n", entry)
			hash := sha256.New()
			f, _ := os.OpenFile("/home/pheckel/Code/ceph/client/a/" + strconv.Itoa(i), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)

			remaining := entry.fileSize

			for _, fragment := range entry.runs {
				if fragment.sparse {
					b := make([]byte, clusterSize)

					for i := 0; i < int(fragment.clusterCount); i++ {
						_, err := f.Write(b)
						if err != nil {
							panic(err)
						}

						hash.Write(b)
						remaining -= clusterSize
					}
				} else {
					bufferSize := int64(math.Min(float64(remaining), float64(fragment.toOffset - fragment.fromOffset)))
					remaining -= uint64(bufferSize)

					b := make([]byte, bufferSize)
					n, err := fs.ReadAt(b, int64(fragment.fromOffset))
					if err != nil {
						panic(err)
					}
					if n != len(b) {
						panic("invalid read")
					}

					_, err = f.Write(b)
					if err != nil {
						panic(err)
					}

					hash.Write(b)
				}

			}

			f.Close()
			fmt.Printf("hash = %x\n\n", hash.Sum(nil))
		}
	}

	m := &internal.ManifestV1{}
	m.Size = uint64(stat.Size())

}