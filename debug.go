package fsdup

import "fmt"

var (
	Debug = false // Toggle with -debug CLI flag! // TODO fix this with debug/log levels
)

func Debugf(format string, args ...interface{}) {
	if Debug {
		fmt.Printf(format, args...)
	}
}
