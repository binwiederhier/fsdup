package main
/*
import (
	"fmt"
	"heckel.io/fsdup/internal"
	"os"
)

type fixedChunker struct {
	size int64
	skip *diskManifest
	out *diskManifest
}

func NewFixedChunker(size int64, skip *diskManifest) *fixedChunker {
	return &fixedChunker{
		size: size,
		skip: skip,
		out: NewManifest(),
	}
}

func (d *fixedChunker) Dedup() (*diskManifest, error) {


	// TODO replace this with diskManifest, then re-use in index.go for disk chunking



	// Sort breakpoints to prepare for sequential disk traversal
	breakpoints := d.skip.Breakpoints()

	// We have determined the breakpoints for the FILE entries.
	// Now, we'll process the non-FILE data to fill in the gaps.

	currentOffset := int64(0)
	breakpointIndex := 0
	breakpoint := int64(0)

	chunk := NewChunk()
	buffer := make([]byte, chunkSizeMaxBytes)

	os.Mkdir("index", 0770)

	for currentOffset < d.size {
		hasNextBreakpoint := breakpointIndex < len(breakpoints)

		if hasNextBreakpoint {
			// At this point, we figure out if the space from the current offset to the
			// next breakpoint will fit in a full chunk.

			breakpoint = breakpoints[breakpointIndex]
			bytesToBreakpoint := breakpoint - currentOffset

			if bytesToBreakpoint > chunkSizeMaxBytes {
				// We can fill an entire chunk, because there are enough bytes to the next breakpoint

				chunkEndOffset := minInt64(currentOffset + chunkSizeMaxBytes, d.size)

				bytesRead, err := d.reader.ReadAt(buffer, d.start + currentOffset)
				if err != nil {
					return nil, err
				} else if bytesRead != chunkSizeMaxBytes {
					return nil, fmt.Errorf("cannot read all bytes from disk, %d read\n", bytesRead)
				}

				chunk.Reset()
				chunk.Write(buffer[:bytesRead])

				if _, ok := d.chunkMap[chunk.ChecksumString()]; !ok {
					if err := d.writeChunkFile(chunk); err != nil {
						return err
					}

					d.chunkMap[chunk.ChecksumString()] = true
				}

				Debugf("offset %d - %d, NEW chunk %x, size %d\n",
					currentOffset, chunkEndOffset, chunk.Checksum(), chunk.Size())

				d.manifest.Slices = append(d.manifest.Slices, &internal.Slice{
					Checksum: chunk.Checksum(),
					Offset: 0,
					Length: chunk.Size(),
				})

				currentOffset = chunkEndOffset
			} else {
				// There are NOT enough bytes to the next breakpoint to fill an entire chunk

				if bytesToBreakpoint > 0 {
					// Create and emit a chunk from the current position to the breakpoint.
					// This may create small chunks and is inefficient.
					// FIXME this should just buffer the current chunk and not emit is right away. It should FILL UP a chunk later!

					bytesRead, err := d.reader.ReadAt(buffer[:bytesToBreakpoint], d.start + currentOffset)
					if err != nil {
						return err
					} else if int64(bytesRead) != bytesToBreakpoint {
						return fmt.Errorf("cannot read all bytes from disk, %d read\n", bytesRead)
					}

					chunk.Reset()
					chunk.Write(buffer[:bytesRead])

					if _, ok := d.chunkMap[chunk.ChecksumString()]; !ok {
						if err := d.writeChunkFile(chunk); err != nil {
							return err
						}

						d.chunkMap[chunk.ChecksumString()] = true
					}

					d.manifest.Slices = append(d.manifest.Slices, &internal.Slice{
						Checksum: chunk.Checksum(),
						Offset: 0,
						Length: chunk.Size(),
					})

					Debugf("offset %d - %d, NEW2 chunk %x, size %d\n",
						currentOffset, currentOffset + bytesToBreakpoint, chunk.Checksum(), chunk.Size())

					currentOffset += bytesToBreakpoint
				}

				// Now we are AT the breakpoint.
				// Simply add this entry to the manifest.

				part := d.diskMap[breakpoint]
				partSize := part.to - part.from

				Debugf("offset %d - %d, size %d  -> FILE chunk %x, offset %d - %d\n",
					currentOffset, currentOffset + partSize, partSize, part.checksum, part.from, part.to)

				d.manifest.Slices = append(d.manifest.Slices, &internal.Slice{
					Checksum: part.checksum,
					Offset: part.from,
					Length: partSize,
				})

				currentOffset += partSize
				breakpointIndex++
			}
		} else {
			chunkEndOffset := minInt64(currentOffset + chunkSizeMaxBytes, d.sizeInBytes)
			chunkSize := chunkEndOffset - currentOffset

			bytesRead, err := d.reader.ReadAt(buffer[:chunkSize], d.start + currentOffset)
			if err != nil {
				panic(err)
			} else if int64(bytesRead) != chunkSize {
				panic(fmt.Errorf("cannot read bytes from disk, %d read\n", bytesRead))
			}

			chunk.Reset()
			chunk.Write(buffer[:bytesRead])

			if _, ok := d.chunkMap[chunk.ChecksumString()]; !ok {
				if err := d.writeChunkFile(chunk); err != nil {
					return err
				}

				d.chunkMap[chunk.ChecksumString()] = true
			}

			Debugf("offset %d - %d, NEW3 chunk %x, size %d\n",
				currentOffset, chunkEndOffset, chunk.Checksum(), chunk.Size())

			d.manifest.Slices = append(d.manifest.Slices, &internal.Slice{
				Checksum: chunk.Checksum(),
				Offset: 0,
				Length: chunk.Size(),
			})

			currentOffset = chunkEndOffset
		}
	}

	return nil
}
*/