package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"heckel.io/fsdup/internal"
	"io/ioutil"
	"sort"
)

type diskManifest struct {
	diskMap map[int64]*chunkPart
	size    int64
}

type chunkPart struct {
	checksum []byte
	from int64
	to int64
}

func NewManifest() *diskManifest {
	return &diskManifest{
		diskMap: make(map[int64]*chunkPart, 0),
	}
}

// Breakpoints returns a sorted list of breakpoints, useful for sequential disk traversal
func (m *diskManifest) Breakpoints() []int64 {
	breakpoints := make([]int64, 0, len(m.diskMap))
	for breakpoint, _ := range m.diskMap {
		breakpoints = append(breakpoints, breakpoint)
	}

	sort.Slice(breakpoints, func(i, j int) bool {
		return breakpoints[i] < breakpoints[j]
	})

	return breakpoints
}

func (m *diskManifest) Add(offset int64, part *chunkPart) {
	m.diskMap[offset] = part
	m.size = maxInt64(m.size, offset + part.to - part.from)
}

func (m *diskManifest) Get(offset int64) *chunkPart {
	return m.diskMap[offset]
}

func (m *diskManifest) Merge(other *diskManifest) {
	for offset, part := range other.diskMap {
		m.diskMap[offset] = part
	}
}

func (m *diskManifest) WriteToFile(file string) error {
	// Transform to protobuf struct
	manifest := &internal.ManifestV1{
		Size: m.size,
		Slices: make([]*internal.Slice, len(m.diskMap)),
	}

	for i, offset := range m.Breakpoints() {
		part := m.diskMap[offset]
		manifest.Slices[i] = &internal.Slice{
			Checksum: part.checksum,
			Offset: part.from,
			Length: part.to - part.from,
		}
	}

	// Save to file
	buffer, err := proto.Marshal(manifest)
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(file, buffer, 0644); err != nil {
		return err
	}

	return nil
}

func (m *diskManifest) Print() {
	for _, offset := range m.Breakpoints() {
		part := m.diskMap[offset]

		if part.checksum == nil {
			fmt.Printf("diskoff %013d - %013d len %-10d -> sparse\n",
				offset, offset + part.to - part.from, part.to - part.from)
		} else {
			fmt.Printf("diskoff %013d - %013d len %-10d -> chunk %64x chunkoff %10d - %10d\n",
				offset, offset + part.to - part.from, part.to - part.from, part.checksum, part.from, part.to)
		}
	}
}

func readManifestFromFile(manifestFile string) (*internal.ManifestV1, error) {
	in, err := ioutil.ReadFile(manifestFile)
	if err != nil {
		return nil, err
	}

	manifest := &internal.ManifestV1{}
	if err := proto.Unmarshal(in, manifest); err != nil {
		return nil, err
	}

	return manifest, nil
}
