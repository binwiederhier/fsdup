package fsdup

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"heckel.io/fsdup/pb"
	"io/ioutil"
	"sort"
)

type kind int

const (
	kindFile kind = 1
	kindSparse kind = 2
	kindGap kind = 3
)

type manifest struct {
	diskMap map[int64]*chunkSlice
	size    int64
}

type chunkSlice struct {
	checksum []byte
	from int64
	to int64
	kind kind
}

func NewManifest() *manifest {
	return &manifest{
		size: 0,
		diskMap: make(map[int64]*chunkSlice, 0),
	}
}

func NewManifestFromFile(file string) (*manifest, error) {
	in, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	pbmanifest := &pb.ManifestV1{}
	if err := proto.Unmarshal(in, pbmanifest); err != nil {
		return nil, err
	}

	manifest := NewManifest()
	offset := int64(0)

	for _, slice := range pbmanifest.Slices {
		manifest.Add(offset, &chunkSlice{
			checksum: slice.Checksum,
			from: slice.Offset,
			to: slice.Offset + slice.Length,
			kind: kind(slice.Kind),
		})

		offset += slice.Length
	}

	return manifest, nil
}

// Breakpoints returns a sorted list of breakpoints, useful for sequential disk traversal
func (m *manifest) Offsets() []int64 {
	offsets := make([]int64, 0, len(m.diskMap))
	for breakpoint, _ := range m.diskMap {
		offsets = append(offsets, breakpoint)
	}

	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})

	return offsets
}

// Chunks returns a map of chunks in this manifest. It does not contain the chunk data.
// The key is a hex representation of the chunk checksum.
func (m *manifest) Chunks() map[string]*chunk {
	chunkMap := make(map[string]*chunk, 0)

	for _, part := range m.diskMap {
		// This is a weird way to get the chunk size, but hey ...
		checksumStr := fmt.Sprintf("%x", part.checksum)

		if _, ok := chunkMap[checksumStr]; !ok {
			chunkMap[checksumStr] = &chunk{
				checksum: part.checksum,
				size: part.to,
			}
		} else {
			chunkMap[checksumStr].size = maxInt64(chunkMap[checksumStr].size, part.to)
		}
	}

	return chunkMap
}

func (m *manifest) Add(offset int64, part *chunkSlice) {
	m.diskMap[offset] = part
}

func (m *manifest) Get(offset int64) *chunkSlice {
	return m.diskMap[offset]
}

func (m *manifest) Size() int64 {
	size := int64(0)

	for offset, _ := range m.diskMap {
		part := m.diskMap[offset]
		size = maxInt64(size, offset + part.to - part.from)
	}

	return size
}

func (m *manifest) Merge(other *manifest) {
	for offset, part := range other.diskMap {
		m.diskMap[offset] = part
	}
}

func (m *manifest) MergeAtOffset(offset int64, other *manifest) {
	for sliceOffset, part := range other.diskMap {
		m.diskMap[offset+sliceOffset] = part
	}
}

func (m *manifest) WriteToFile(file string) error {
	// Transform to protobuf struct
	pbmanifest := &pb.ManifestV1{
		Size: m.Size(),
		Slices: make([]*pb.Slice, len(m.diskMap)),
	}

	for i, offset := range m.Offsets() {
		slice := m.diskMap[offset]
		pbmanifest.Slices[i] = &pb.Slice{
			Checksum: slice.checksum,
			Offset:   slice.from,
			Length:   slice.to - slice.from,
			Kind:     int32(slice.kind),
		}
	}

	// Save to file
	buffer, err := proto.Marshal(pbmanifest)
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(file, buffer, 0644); err != nil {
		return err
	}

	return nil
}

func (m *manifest) Print() {
	for i, offset := range m.Offsets() {
		slice := m.diskMap[offset]

		if slice.checksum == nil {
			fmt.Printf("idx %010d diskoff %013d - %013d len %-13d sparse     -\n",
				i, offset, offset + slice.to - slice.from, slice.to - slice.from)
		} else {
			kind := "unknown"
			if slice.kind == kindGap {
				kind = "gap"
			} else if slice.kind == kindFile {
				kind = "file"
			}

			fmt.Printf("idx %010d diskoff %013d - %013d len %-13d %-10s chunk %64x chunkoff %10d - %10d\n",
				i, offset, offset + slice.to - slice.from, slice.to - slice.from, kind, slice.checksum, slice.from, slice.to)
		}
	}
}
