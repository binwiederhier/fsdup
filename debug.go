package main

import "fmt"

var (
	debug = true
)

func Printf(fmt_str string, args ...interface{}) {
	if debug {
		fmt.Printf(fmt_str, args...)
	}
}
