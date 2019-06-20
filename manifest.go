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

type diskSlice struct {
	diskfrom  int64
	diskto    int64
	checksum  []byte
	chunkfrom int64
	chunkto   int64
	length    int64
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
	for offset, _ := range m.diskMap {
		offsets = append(offsets, offset)
	}

	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})

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
				size:     slice.to,
			}
		} else {
			chunks[checksumStr].size = maxInt64(chunks[checksumStr].size, slice.to)
		}
	}

	return chunks
}

// ChunkSlices returns a map of chunks and its sections on disk.
// The key is a hex representation of the chunk checksum.

func (m *manifest) ChunkSlices() (map[string][]*diskSlice, error) {
	// Create the better manifest
	manifest := make(map[int64]*diskSlice, 0)
	for offset, slice := range m.diskMap {
		manifest[offset] = &diskSlice{
			diskfrom: offset,
			diskto: offset + slice.to - slice.from,
			checksum: slice.checksum,
			chunkfrom: slice.from,
			chunkto: slice.to,
			length: slice.to - slice.from,
		}
	}

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

	chunkSlices := make(map[string][]*diskSlice, 0)

	for _, slice := range manifest {
		if slice.checksum == nil {
			continue
		}

		checksumStr := fmt.Sprintf("%x", slice.checksum)

		if _, ok := chunkSlices[checksumStr]; !ok {
			chunkSlices[checksumStr] = make([]*diskSlice, 0)
		}

		chunkSlices[checksumStr] = append(chunkSlices[checksumStr], slice)
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

	for _, diskSlices := range chunkSlices {
		sort.Slice(diskSlices, func(i, j int) bool {
			if diskSlices[i].chunkfrom > diskSlices[j].chunkfrom {
				return false
			} else if diskSlices[i].chunkfrom < diskSlices[j].chunkfrom {
				return true
			} else {
				return diskSlices[i].chunkto > diskSlices[j].chunkto
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

	for checksumStr, diskSlices := range chunkSlices {
		if len(diskSlices) == 1 {
			continue
		}

		newDiskSlices := make([]*diskSlice, 1)
		newDiskSlices[0] = diskSlices[0]

		for c, n := 0, 1; c < len(diskSlices) && n < len(diskSlices); n++ {
			current := diskSlices[c]
			next := diskSlices[n]

			if current.chunkto == next.chunkfrom {
				newDiskSlices = append(newDiskSlices, next)
				c = n
			}
		}

		chunkSlices[checksumStr] = newDiskSlices
	}

	return chunkSlices, nil
}

// Add adds a chunk slice to the manifest at the given from
func (m *manifest) Add(offset int64, part *chunkSlice) {
	m.diskMap[offset] = part
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
		size = maxInt64(size, offset + slice.to - slice.from)
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
