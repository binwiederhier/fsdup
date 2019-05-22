package main

import "fmt"

var (
	debug = true
)

func Debugf(format string, args ...interface{}) {
	if debug {
		fmt.Printf(format, args...)
	}
}
