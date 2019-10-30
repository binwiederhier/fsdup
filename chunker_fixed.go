package fsdup

import (
	"fmt"
	"io"
	"sync"
)

type fixedChunker struct {
	reader           io.ReaderAt
	store            ChunkStore
	start            int64
	sizeInBytes      int64
	chunkMaxSize     int64
	writeConcurrency int64
	skip             *manifest
}

func NewFixedChunker(reader io.ReaderAt, index ChunkStore, offset int64, size int64, chunkMaxSize int64,
	writeConcurrency int64) *fixedChunker {

	skip := NewManifest(chunkMaxSize)
	return NewFixedChunkerWithSkip(reader, index, offset, size, chunkMaxSize, writeConcurrency, skip)
}

func NewFixedChunkerWithSkip(reader io.ReaderAt, store ChunkStore, offset int64, size int64,
	chunkMaxSize int64, writeConcurrency int64, skip *manifest) *fixedChunker {

	return &fixedChunker{
		reader:           reader,
		store:            store,
		start:            offset,
		sizeInBytes:      size,
		chunkMaxSize:     chunkMaxSize,
		writeConcurrency: writeConcurrency,
		skip:             skip,
	}
}

func (d *fixedChunker) Dedup() (*manifest, error) {
	out := NewManifest(d.chunkMaxSize)

	sliceOffsets := d.skip.Offsets()

	currentOffset := int64(0)
	breakpointIndex := 0
	breakpoint := int64(0)

	wg := sync.WaitGroup{}
	writeChan := make(chan *chunk)
	errChan := make(chan error)
	chunkPool := &sync.Pool{
		New: func() interface{} {
			return NewChunk(d.chunkMaxSize)
		},
	}

	for i := int64(0); i < d.writeConcurrency; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()

			for c := range writeChan {
				if err := d.store.Write(c.Checksum(), c.Data()); err != nil {
					errChan <- err
				}
				chunkPool.Put(c)
			}
		}()
	}

	buffer := make([]byte, d.chunkMaxSize)

	statusf("Creating gap chunks ...")
	chunkBytes := int64(0)
	chunkCount := int64(0)

	for currentOffset < d.sizeInBytes {
		select {
		case err := <-errChan:
			return nil, err
		default:
		}

		hasNextBreakpoint := breakpointIndex < len(sliceOffsets)

		if hasNextBreakpoint {
			// At this point, we figure out if the space from the current offset to the
			// next breakpoint will fit in a full chunk.

			breakpoint = sliceOffsets[breakpointIndex]
			bytesToBreakpoint := breakpoint - currentOffset

			if bytesToBreakpoint > d.chunkMaxSize {
				// We can fill an entire chunk, because there are enough bytes to the next breakpoint

				chunkEndOffset := minInt64(currentOffset + d.chunkMaxSize, d.sizeInBytes)

				bytesRead, err := d.reader.ReadAt(buffer, d.start + currentOffset)
				if err != nil {
					return nil, err
				} else if int64(bytesRead) != d.chunkMaxSize {
					return nil, fmt.Errorf("cannot read all bytes from disk, %d read\n", bytesRead)
				}

				achunk := chunkPool.Get().(*chunk)
				achunk.Reset()
				achunk.Write(buffer[:bytesRead])

				debugf("offset %d - %d, NEW chunk %x, size %d\n",
					currentOffset, chunkEndOffset, achunk.Checksum(), achunk.Size())

				out.Add(&chunkSlice{
					checksum:  achunk.Checksum(),
					kind:      kindGap,
					diskfrom:  currentOffset,
					diskto:    currentOffset + achunk.Size(),
					chunkfrom: 0,
					chunkto:   achunk.Size(),
					length:    achunk.Size(),
				})

				chunkBytes += achunk.Size()
				chunkCount++
				statusf("Creating gap chunk(s) (%d chunk(s), %s) ...", chunkCount, convertBytesToHumanReadable(chunkBytes))

				writeChan <- achunk
				currentOffset = chunkEndOffset
			} else {
				// There are NOT enough bytes to the next breakpoint to fill an entire chunk

				if bytesToBreakpoint > 0 {
					// Create and emit a chunk from the current position to the breakpoint.
					// This may create small chunks and is inefficient.
					// FIXME this should just buffer the current chunk and not emit is right away. It should FILL UP a chunk later!

					bytesRead, err := d.reader.ReadAt(buffer[:bytesToBreakpoint], d.start + currentOffset)
					if err != nil {
						return nil, err
					} else if int64(bytesRead) != bytesToBreakpoint {
						return nil, fmt.Errorf("cannot read all bytes from disk, %d read\n", bytesRead)
					}

					achunk := chunkPool.Get().(*chunk)
					achunk.Reset()
					achunk.Write(buffer[:bytesRead])

					out.Add(&chunkSlice{
						checksum:  achunk.Checksum(),
						kind:      kindGap,
						diskfrom:  currentOffset,
						diskto:    currentOffset + achunk.Size(),
						chunkfrom: 0,
						chunkto:   achunk.Size(),
						length:    achunk.Size(),
					})

					chunkBytes += achunk.Size()
					chunkCount++
					statusf("Creating gap chunk(s) (%d chunk(s), %s) ...", chunkCount, convertBytesToHumanReadable(chunkBytes))

					debugf("offset %d - %d, NEW2 chunk %x, size %d\n",
						currentOffset, currentOffset + bytesToBreakpoint, achunk.Checksum(), achunk.Size())

					writeChan <- achunk
					currentOffset += bytesToBreakpoint
				}

				// Now we are AT the breakpoint.
				// Simply add this entry to the manifest.

				part := d.skip.Get(breakpoint)
				partSize := part.chunkto - part.chunkfrom

				debugf("offset %d - %d, size %d  -> FILE chunk %x, offset %d - %d\n",
					currentOffset, currentOffset + partSize, partSize, part.checksum, part.chunkfrom, part.chunkto)

				currentOffset += partSize
				breakpointIndex++
			}
		} else {
			chunkEndOffset := minInt64(currentOffset + d.chunkMaxSize, d.sizeInBytes)
			chunkSize := chunkEndOffset - currentOffset

			bytesRead, err := d.reader.ReadAt(buffer[:chunkSize], d.start + currentOffset)
			if err != nil {
				panic(err)
			} else if int64(bytesRead) != chunkSize {
				panic(fmt.Errorf("cannot read bytes from disk, %d read\n", bytesRead))
			}

			achunk := chunkPool.Get().(*chunk)
			achunk.Reset()
			achunk.Write(buffer[:bytesRead])

			debugf("offset %d - %d, NEW3 chunk %x, size %d\n",
				currentOffset, chunkEndOffset, achunk.Checksum(), achunk.Size())

			out.Add(&chunkSlice{
				checksum:  achunk.Checksum(),
				kind:      kindGap,
				diskfrom:  currentOffset,
				diskto:    currentOffset + achunk.Size(),
				chunkfrom: 0,
				chunkto:   achunk.Size(),
				length:    achunk.Size(),
			})

			chunkBytes += achunk.Size()
			chunkCount++
			statusf("Indexing gap chunks (%d chunk(s), %s) ...", chunkCount, convertBytesToHumanReadable(chunkBytes))

			writeChan <- achunk
			currentOffset = chunkEndOffset
		}
	}

	statusf("Waiting for remaining chunk writes ...")
	close(writeChan)
	wg.Wait()

	statusf("Indexed %s of gaps (%d chunk(s))\n", convertBytesToHumanReadable(chunkBytes), chunkCount)
	return out, nil
}
