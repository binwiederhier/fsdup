package main

type chunkStore interface {
	WriteChunk(chunk *chunk) error
}
