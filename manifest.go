package main

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
	diskMap map[int64]*chunkPart
	size    int64
}

type chunkPart struct {
	checksum []byte
	from int64
	to int64
	kind kind
}

func NewManifest() *manifest {
	return &manifest{
		size: 0,
		diskMap: make(map[int64]*chunkPart, 0),
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
		manifest.Add(offset, &chunkPart{
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
func (m *manifest) Breakpoints() []int64 {
	breakpoints := make([]int64, 0, len(m.diskMap))
	for breakpoint, _ := range m.diskMap {
		breakpoints = append(breakpoints, breakpoint)
	}

	sort.Slice(breakpoints, func(i, j int) bool {
		return breakpoints[i] < breakpoints[j]
	})

	return breakpoints
}

func (m *manifest) Add(offset int64, part *chunkPart) {
	m.diskMap[offset] = part
}

func (m *manifest) Get(offset int64) *chunkPart {
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
	for partOffset, part := range other.diskMap {
		m.diskMap[offset+partOffset] = part
	}
}

func (m *manifest) WriteToFile(file string) error {
	// Transform to protobuf struct
	pbmanifest := &pb.ManifestV1{
		Size: m.Size(),
		Slices: make([]*pb.Slice, len(m.diskMap)),
	}

	for i, offset := range m.Breakpoints() {
		part := m.diskMap[offset]
		pbmanifest.Slices[i] = &pb.Slice{
			Checksum: part.checksum,
			Offset: part.from,
			Length: part.to - part.from,
			Kind: int32(part.kind),
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
	for _, offset := range m.Breakpoints() {
		part := m.diskMap[offset]

		if part.checksum == nil {
			fmt.Printf("kind%d diskoff %013d - %013d len %-10d -> sparse\n",
				part.kind, offset, offset + part.to - part.from, part.to - part.from)
		} else {
			fmt.Printf("kind%d diskoff %013d - %013d len %-10d -> chunk %64x chunkoff %10d - %10d\n",
				part.kind, offset, offset + part.to - part.from, part.to - part.from, part.checksum, part.from, part.to)
		}
	}
}
