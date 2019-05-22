package main

import (
	"errors"
	"os"
)

func index(filename string, manifest string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}

	defer file.Close()

	// Determine format
	// ...

	stat, err := file.Stat()
	if err != nil {
		return errors.New("cannot read file")
	}

	return parseNTFS(file, stat.Size(), manifest)
}