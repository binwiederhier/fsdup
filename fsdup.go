package main

import (
	"flag"
	"fmt"
	"os"
)

// TODO [HIGH] Support for MBR/GPT partition tables
// TODO [HIGH] Support for any file
// TODO [MED] Make manifest creation the last step (or part of all steps)
// TODO [LOW] Sparsify all runs automatically

func exit(code int, message string) {
	fmt.Println(message)
	os.Exit(code)
}

func usage() {
	fmt.Println("Syntax:")
	fmt.Println("  fsdup index [-debug] [-nowrite] [-offset OFFSET] [-exact] INFILE MANIFEST")
	fmt.Println("  fsdup map [-debug] MANIFEST")
	fmt.Println("  fsdup export [-debug] MANIFEST OUTFILE")
	fmt.Println("  fsdup print [-debug] MANIFEST")
	fmt.Println("  fsdup stat [-debug] MANIFEST...")

	os.Exit(1)
}

func main() {
	indexCommand := flag.NewFlagSet("index", flag.ExitOnError)
	indexDebugFlag := indexCommand.Bool("debug", debug, "Enable debug mode")
	indexNoWriteFlag := indexCommand.Bool("nowrite", false, "Do not write chunk data, only manifest")
	indexOffset := indexCommand.Int64("offset", 0, "Start reading file at given offset")
	indexExact := indexCommand.Bool("exact", false, "Ignore the NTFS bitmap, i.e. include unused blocks")

	mapCommand := flag.NewFlagSet("map", flag.ExitOnError)
	mapDebugFlag := mapCommand.Bool("debug", debug, "Enable debug mode")

	exportCommand := flag.NewFlagSet("export", flag.ExitOnError)
	exportDebugFlag := exportCommand.Bool("debug", debug, "Enable debug mode")

	printCommand := flag.NewFlagSet("print", flag.ExitOnError)
	printDebugFlag := printCommand.Bool("debug", debug, "Enable debug mode")

	statCommand := flag.NewFlagSet("stat", flag.ExitOnError)
	statDebugFlag := statCommand.Bool("debug", debug, "Enable debug mode")

	if len(os.Args) < 2 {
		usage()
	}

	command := os.Args[1]

	switch command {
	case "index":
		indexCommand.Parse(os.Args[2:])

		if indexCommand.NArg() < 2 {
			usage()
		}

		debug = *indexDebugFlag
		nowrite := *indexNoWriteFlag
		offset := *indexOffset
		exact := *indexExact

		file := indexCommand.Arg(0)
		manifest := indexCommand.Arg(1)

		if err := index(file, manifest, offset, nowrite, exact); err != nil {
			exit(2, "Cannot index file: " + string(err.Error()))
		}
	case "map":
		mapCommand.Parse(os.Args[2:])

		if mapCommand.NArg() < 1 {
			usage()
		}

		debug = *mapDebugFlag
		filename := mapCommand.Arg(0)

		mapDevice(filename)
	case "export":
		exportCommand.Parse(os.Args[2:])

		if exportCommand.NArg() < 2 {
			usage()
		}

		debug = *exportDebugFlag
		manifest := exportCommand.Arg(0)
		outfile := exportCommand.Arg(1)

		export(manifest, outfile)
	case "print":
		printCommand.Parse(os.Args[2:])

		if printCommand.NArg() < 1 {
			usage()
		}

		debug = *printDebugFlag
		manifest := printCommand.Arg(0)

		if err := printManifestFile(manifest); err != nil {
			exit(2, "Cannot read manifest: " + string(err.Error()))
		}
	case "stat":
		statCommand.Parse(os.Args[2:])

		if statCommand.NArg() < 1 {
			usage()
		}

		debug = *statDebugFlag
		manifests := statCommand.Args()

		if err := printManifestStats(manifests); err != nil {
			exit(2, "Cannot create manifest stats: " + string(err.Error()))
		}
	default:
		usage()
	}
}
