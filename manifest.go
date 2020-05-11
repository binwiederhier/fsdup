package fsdup

import (
	"errors"
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
	diskMap      map[int64]*chunkSlice
	size         int64
	chunkMaxSize int64
	offsets      []int64 // cache, don't forget to update for all write operations!
}

type chunkSlice struct {
	checksum  []byte
	kind      kind
	diskfrom  int64
	diskto    int64
	chunkfrom int64
	chunkto   int64
	length    int64
}

func NewManifest(chunkMaxSize int64) *manifest {
	return &manifest{
		size: 0,
		chunkMaxSize: chunkMaxSize,
		diskMap: make(map[int64]*chunkSlice, 0),
		offsets: nil,
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

	return NewManifestFromProto(pbmanifest)
}

func NewManifestFromProto(pbmanifest *pb.ManifestV1) (*manifest, error) {
	chunkMaxSize := int64(DefaultChunkSizeMaxBytes)
	if pbmanifest.ChunkMaxSize != 0 {
		chunkMaxSize = pbmanifest.ChunkMaxSize
	}

	manifest := NewManifest(chunkMaxSize)
	offset := int64(0)

	for _, slice := range pbmanifest.Slices {
		manifest.Add(&chunkSlice{
			checksum:  slice.Checksum,
			kind:      kind(slice.Kind),
			diskfrom:  offset,
			diskto:    offset + slice.Length,
			chunkfrom: slice.Offset,
			chunkto:   slice.Offset + slice.Length,
			length:    slice.Length,
		})

		offset += slice.Length
	}

	return manifest, nil
}

// Breakpoints returns a sorted list of slice offsets, useful for sequential disk traversal
func (m *manifest) Offsets() []int64 {
	if m.offsets != nil {
		return m.offsets
	}

	offsets := make([]int64, 0, len(m.diskMap))
	for offset, _ := range m.diskMap {
		offsets = append(offsets, offset)
	}

	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})

	m.offsets = offsets
	return offsets
}

// Chunks returns a map of chunks in this manifest. It does not contain the chunk data.
// The key is a hex representation of the chunk checksum.
func (m *manifest) Chunks() map[string]*chunk {
	chunks := make(map[string]*chunk, 0)

	for _, slice := range m.diskMap {
		// This is a weird way to get the chunk size, but hey ...
		checksumStr := fmt.Sprintf("%x", slice.checksum)

		if _, ok := chunks[checksumStr]; !ok {
			chunks[checksumStr] = &chunk{
				checksum: slice.checksum,
				size:     slice.chunkto,
			}
		} else {
			chunks[checksumStr].size = maxInt64(chunks[checksumStr].size, slice.chunkto)
		}
	}

	return chunks
}

// SlicesBetween efficiently finds the slices storing the given disk offset
func (m *manifest) SlicesBetween(from int64, to int64) ([]*chunkSlice, error) {
	offsets := m.Offsets()

	fromIndex := sort.Search(len(offsets), func(i int) bool {
		return i+1 == len(offsets) || offsets[i+1] > from
	})

	toIndex := sort.Search(len(offsets), func(i int) bool {
		return i+1 == len(offsets) || offsets[i+1] > to
	})

	if fromIndex == len(offsets) || toIndex == len(offsets) {
		return nil, errors.New("cannot find slice at given offset")
	}

	slices := make([]*chunkSlice, 0)

	for i := fromIndex; i <= toIndex; i++ {
		slices = append(slices, m.diskMap[offsets[i]])
	}

	return slices, nil
}

// Slices returns a map of chunks and its sections on disk.
// The key is a hex representation of the chunk checksum.
func (m *manifest) ChunkSlices() (map[string][]*chunkSlice, error) {
	// First, we'll sort all slices into a map grouped by chunk checksum. This
	// produces a map with potentially overlapping slices:
	//
	//   slices[aabbee..] = (
	//       (from:16384,  to:36864),
	//       (from:0,      to:8192),
	//       (from:8192,   to:16384),
	//       (from:36864,  to:65536),
	//       (from:0,      to:16384),    < overlaps with two slices
	//   )

	checksumSlicesMap := make(map[string][]*chunkSlice, 0)

	for _, slice := range m.diskMap {
		if slice.checksum == nil {
			continue
		}

		checksumStr := fmt.Sprintf("%x", slice.checksum)

		if _, ok := checksumSlicesMap[checksumStr]; !ok {
			checksumSlicesMap[checksumStr] = make([]*chunkSlice, 0)
		}

		checksumSlicesMap[checksumStr] = append(checksumSlicesMap[checksumStr], slice)
	}

	// Now, we'll sort each disk slice list by "from" (smallest first),
	// and if the "from" fields are equal, by the "to" field (highest first);
	// this is to prefer larger sections.
	//
	//   slices[aabbee..] = (
	//       (from:0,      to:16384),    < overlaps with next two slices
	//       (from:0,      to:8192),
	//       (from:8192,   to:16384),
	//       (from:16384,  to:36864),
	//       (from:36864,  to:65536),
	//   )

	for _, slices := range checksumSlicesMap {
		sort.Slice(slices, func(i, j int) bool {
			if slices[i].chunkfrom > slices[j].chunkfrom {
				return false
			} else if slices[i].chunkfrom < slices[j].chunkfrom {
				return true
			} else {
				return slices[i].chunkto > slices[j].chunkto
			}
		})
	}

	// Now, we walk the list and find connecting pieces
	//
	//   slices[aabbee..] = (
	//       (from:0,      to:16384),
	//       (from:16384,  to:36864),
	//       (from:36864,  to:65536),
	//   )

	for checksumStr, slices := range checksumSlicesMap {
		if len(slices) == 1 {
			continue
		}

		newSlices := make([]*chunkSlice, 1)
		newSlices[0] = slices[0]

		for c, n := 0, 1; c < len(slices) && n < len(slices); n++ {
			current := slices[c]
			next := slices[n]

			if current.chunkto == next.chunkfrom {
				newSlices = append(newSlices, next)
				c = n
			}
		}

		checksumSlicesMap[checksumStr] = newSlices
	}

	return checksumSlicesMap, nil
}

// ChecksumsByDiskOffset orders the given list by first slice disk offset. This
// is useful to read all chunks as sequential as possible.
func (m *manifest) ChecksumsByDiskOffset(chunkSlices map[string][]*chunkSlice) []string {
	checksumStrs := make([]string, 0)
	for checksumStr, _ := range chunkSlices {
		checksumStrs = append(checksumStrs, checksumStr)
	}

	sort.Slice(checksumStrs, func(i, j int) bool {
		return chunkSlices[checksumStrs[i]][0].diskfrom < chunkSlices[checksumStrs[j]][0].diskfrom
	})

	return checksumStrs
}

// Add adds a chunk slice to the manifest at the given from
func (m *manifest) Add(slice *chunkSlice) {
	m.diskMap[slice.diskfrom] = slice
	m.resetCaches()
}

// Get receives a chunk slice from the manifest at the given from.
// Note that the from must match exactly. No soft matching is performed.
func (m *manifest) Get(offset int64) *chunkSlice {
	return m.diskMap[offset]
}

func (m *manifest) Size() int64 {
	size := int64(0)

	for offset, _ := range m.diskMap {
		slice := m.diskMap[offset]
		size = maxInt64(size, offset + slice.chunkto- slice.chunkfrom)
	}

	return size
}

func (m *manifest) Merge(other *manifest) {
	for offset, part := range other.diskMap {
		m.diskMap[offset] = part
	}

	m.resetCaches()
}

func (m *manifest) MergeAtOffset(offset int64, other *manifest) {
	for sliceOffset, part := range other.diskMap {
		m.diskMap[offset+sliceOffset] = part
	}

	m.resetCaches()
}

func (m *manifest) WriteToFile(file string) error {
	buffer, err := proto.Marshal(m.Proto())
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(file, buffer, 0644); err != nil {
		return err
	}

	return nil
}

func (m *manifest) Proto() *pb.ManifestV1 {
	pbmanifest := &pb.ManifestV1{
		Size: m.Size(),
		Slices: make([]*pb.Slice, len(m.diskMap)),
		ChunkMaxSize: m.chunkMaxSize,
	}

	for i, offset := range m.Offsets() {
		slice := m.diskMap[offset]
		pbmanifest.Slices[i] = &pb.Slice{
			Checksum: slice.checksum,
			Offset:   slice.chunkfrom,
			Length:   slice.chunkto - slice.chunkfrom,
			Kind:     int32(slice.kind),
		}
	}

	return pbmanifest
}

func (m *manifest) PrintDisk() {
	for i, offset := range m.Offsets() {
		slice := m.diskMap[offset]

		if slice.checksum == nil {
			fmt.Printf("idx %-10d diskoff %13d - %13d len %-13d sparse     -\n",
				i, offset, offset + slice.chunkto- slice.chunkfrom, slice.chunkto- slice.chunkfrom)
		} else {
			kind := "unknown"
			if slice.kind == kindGap {
				kind = "gap"
			} else if slice.kind == kindFile {
				kind = "file"
			}

			fmt.Printf("idx %-10d diskoff %13d - %13d len %-13d %-10s chunk %64x chunkoff %10d - %10d\n",
				i, offset, offset + slice.chunkto- slice.chunkfrom, slice.chunkto- slice.chunkfrom, kind, slice.checksum, slice.chunkfrom, slice.chunkto)
		}
	}
}

func (m *manifest) PrintChunks() error {
	chunkSlices, err := m.ChunkSlices()
	if err != nil {
		return err
	}

	for _, checksumStr := range m.ChecksumsByDiskOffset(chunkSlices) {
		slices := chunkSlices[checksumStr]
		for i, slice := range slices {
			fmt.Printf("chunk %s idx %-5d diskoff %13d - %13d len %-13d chunkoff %10d - %10d\n",
				checksumStr, i, slice.diskfrom, slice.diskto, slice.length, slice.chunkfrom, slice.chunkto)
		}
	}

	return nil
}

func (m *manifest) resetCaches() {
	m.offsets = nil
}

func (k kind) toString() string {
	if k == kindFile {
		return "file"
	} else if k == kindSparse {
		return "sparse"
	} else if k == kindGap {
		return "gap"
	} else {
		return "unknown"
	}
}

func kindFromString(s string) (kind, error) {
	if s == "file" {
		return kindFile, nil
	} else if s == "sparse" {
		return kindSparse, nil
	} else if s == "gap" {
		return kindGap, nil
	} else {
		return kindFile, errors.New("invalid kind string " + s)
	}
}