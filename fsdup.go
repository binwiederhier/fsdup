package main

import (
	"flag"
	"fmt"
	"os"
)

func exit(code int, message string) {
	fmt.Println(message)
	os.Exit(code)
}

func main() {
	indexCommand := flag.NewFlagSet("index", flag.ExitOnError)
	mapCommand := flag.NewFlagSet("map", flag.ExitOnError)

	if len(os.Args) < 2 {
		exit(1, "Syntax: fsdup index ID FILE\n        fsdup mount ID")
	}

	command := os.Args[1]

	switch command {
	case "index":
		indexCommand.Parse(os.Args[2:])

		if len(os.Args) < 4 {
			exit(1, "Syntax: fsdup index FILE|DISK|PARTITION MANIFEST")
		}

		file := os.Args[2]
		manifest := os.Args[3]

		if err := index(file, manifest); err != nil {
			exit(2, "Cannot index file: " + string(err.Error()))
		}
	case "map":
		mapCommand.Parse(os.Args[2:])

		if len(os.Args) < 3 {
			exit(1, "Syntax: fsdup mount ID")
		}

		filename := os.Args[2]

		mapDevice(filename)
	case "export":
		if len(os.Args) < 4 {
			exit(1, "Syntax: fsdup export MANIFEST OUTFILE")
		}

		manifest := os.Args[2]
		outfile := os.Args[3]

		export(manifest, outfile)
	default:
		flag.PrintDefaults()
		os.Exit(1)
	}
}
