package main

import "fmt"

var (
	debug = false // Toggle with -debug CLI flag!
)

func Debugf(format string, args ...interface{}) {
	if debug {
		fmt.Printf(format, args...)
	}
}
