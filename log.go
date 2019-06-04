package fsdup

import (
	"fmt"
	"strings"
)

var (
	Debug = false // Toggle with -debug CLI flag! // TODO fix this with debug/log levels
	Quiet = false // TODO fix this with log levels

	statusLastLength = 0
)

func debugf(format string, args ...interface{}) {
	if Quiet {
		return
	}

	if Debug {
		fmt.Printf(format, args...)
	}
}

func statusf(format string, args ...interface{}) {
	if Quiet {
		return
	}

	if Debug {
		fmt.Printf(format + "\n", args...)
	} else {
		status := fmt.Sprintf(format, args...)
		statusNewLength := len(status)

		if statusNewLength < statusLastLength {
			fmt.Printf("\r%s\r%s", strings.Repeat(" ", statusLastLength), status)
		} else {
			fmt.Printf("\r%s", status)
		}

		statusLastLength = statusNewLength
	}
}
