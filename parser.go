package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"heckel.io/cephboot/internal"
	"io"
	"os"
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
	fromOffset   uint64
	toOffset     uint64
	firstCluster int64 // signed!
	clusterCount uint64
}

type entry struct {
	allocSize int
	dataRealSize uint64
	runs []run
}

var ErrDataResident = errors.New("data is resident")
var ErrNoDataAttr = errors.New("no data attribute")
var ErrUnexpectedMagic = errors.New("unexpected magic")
var ErrFileTooSmallToIndex = errors.New("file too small to index")

func readRuns(reader io.ReaderAt, clusterSize int64, offset int64) []run {
	runs := make([]run, 0)
	dataRunOffset := offset
	firstCluster := int64(0)

	for {
		dataRunHeader := readUintLE(reader, dataRunOffset, 1)

		if dataRunHeader == 0 {
			break
		}

		clusterCountLength := dataRunHeader & 0x0F
		clusterCountOffset := dataRunOffset + 1
		clusterCount := readUintLE(reader, clusterCountOffset, int64(clusterCountLength))

		firstClusterLength := dataRunHeader & 0xF0 >> 4
		firstClusterOffset := int64(clusterCountOffset) + int64(clusterCountLength)
		firstCluster += readIntLE(reader, int64(firstClusterOffset), int64(firstClusterLength)) // relative to previous, can be negative, so signed!

		fromOffset := int64(firstCluster) * int64(clusterSize)
		toOffset := fromOffset + (int64(clusterCount) + 1) * int64(clusterSize)

		fmt.Printf("data run offset = %d, data run header = 0x%x, data run length length = 0x%x, data run offset length = 0x%x, " +
			"cluster count = %d, first cluster = %d, from offset = %d, to offset = %d\n",
			dataRunOffset, dataRunHeader, clusterCountLength, firstClusterLength, clusterCount, firstCluster,
			fromOffset, toOffset)

		runs = append(runs, run{
			firstCluster: firstCluster,
			clusterCount: clusterCount,
			fromOffset:   uint64(fromOffset),
			toOffset:     uint64(toOffset),
		})

		dataRunOffset = dataRunOffset + int64(firstClusterLength) + int64(clusterCountLength) + 1
	}

	return runs
}

func readEntry(reader io.ReaderAt, clusterSize int64, offset int64) (*entry, error) {
	err := readAndCompare(reader, int64(offset), []byte(ENTRY_MAGIC))
	if err != nil {
		return nil, err
	}

	relativeFirstAttrOffset := readUintLE(reader, offset + ENTRY_FIRST_ATTR_OFFSET, ENTRY_FIRST_ATTR_LENTGH)
	allocatedSize := readUintLE(reader, offset + 28, 4)

	entry := &entry{
		allocSize: int(allocatedSize),
	}

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

/*			if dataRealSize < 2 * 1024 * 1024 {
				return nil, ErrFileTooSmallToIndex
			}*/

			relativeDataRunsOffset := readUintLE(reader, attrOffset + 32, 2)
			dataRunFirstOffset := attrOffset + int64(relativeDataRunsOffset)

			entry.dataRealSize = dataRealSize
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

func readIntLE(reader io.ReaderAt, offset int64, length int64) int64 {
	b := make([]byte, length)
	n, err := reader.ReadAt(b, offset)
	if err != nil {
		panic(err)
	} else if n != len(b) {
		panic("cannot read")
	}

	// FIXME All cases are necessary for datarun offsets and lengths!
	switch length {
	case 0:
		return 0
	case 1:
		return int64(int8(b[0]))
	case 2:
		return int64(int16(binary.LittleEndian.Uint16(b)))
	case 4:
		return int64(int32(binary.LittleEndian.Uint32(b)))
	case 8:
		return int64(binary.LittleEndian.Uint64(b))
	default:
		panic(fmt.Sprintf("invalid length %d", length))
	}

}
func readUintLE(reader io.ReaderAt, offset int64, length int64) uint64 {
	b := make([]byte, length)
	n, err := reader.ReadAt(b, offset)
	if err != nil {
		panic(err)
	} else if n != len(b) {
		panic("cannot read")
	}

	// FIXME All cases are necessary for datarun offsets and lengths!
	switch length {
	case 0:
		return 0
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(b))
	case 4:
		return uint64(binary.LittleEndian.Uint32(b))
	case 8:
		return uint64(binary.LittleEndian.Uint64(b))
	default:
		panic(fmt.Sprintf("invalid length %d", length))
	}
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

		for i := 1; i < int(run.clusterCount); i++ { // Skip entry 0 ($MFT itself!)
			cluster := int64(run.firstCluster) + int64(i)
			clusterOffset := cluster * int64(clusterSize)

			fmt.Printf("reading entry at cluster %d / offset %d:\n", cluster, clusterOffset)

			entry, err := readEntry(fs, int64(clusterSize), clusterOffset)
			if err == ErrDataResident || err == ErrNoDataAttr || err == ErrUnexpectedMagic {
				fmt.Printf("skipping entry: %s\n", err.Error())
				continue
			} else if err != nil {
				panic(err)
			}

			fmt.Printf("entry %#v\n", entry)
		}
	}

	m := &internal.ManifestV1{}
	m.Size = uint64(stat.Size())

}