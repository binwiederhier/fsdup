package main

import (
	"flag"
	"fmt"
	"os"
)

// TODO [LOW] Find zeros in gaps, mark as sparse
// TODO [LOW] Find zeros in FILE runs, mark as sparse
// TODO [LOW] rename "size" to "length"
// TODO [LOW] chunkPart.to|from -> offset|length
// TODO [LOW] different debug levels -d -dd -ddd -q

func exit(code int, message string) {
	fmt.Println(message)
	os.Exit(code)
}

func usage() {
	fmt.Println("Syntax:")
	fmt.Println("  fsdup index [-debug] [-nowrite] [-store STORE] [-offset OFFSET] [-minsize MINSIZE] [-exact] INFILE MANIFEST")
	fmt.Println("  fsdup map [-debug] MANIFEST")
	fmt.Println("  fsdup export [-debug] MANIFEST OUTFILE")
	fmt.Println("  fsdup print [-debug] MANIFEST")
	fmt.Println("  fsdup stat [-debug] MANIFEST...")

	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	command := os.Args[1]

	switch command {
	case "index":
		indexCommand(os.Args[2:])
	case "map":
		mapCommand(os.Args[2:])
	case "export":
		exportCommand(os.Args[2:])
	case "print":
		printCommand(os.Args[2:])
	case "stat":
		statCommand(os.Args[2:])
	default:
		usage()
	}
}

func indexCommand(args []string) {
	flags := flag.NewFlagSet("index", flag.ExitOnError)
	debugFlag := flags.Bool("debug", debug, "Enable debug mode")
	noWriteFlag := flags.Bool("nowrite", false, "Do not write chunk data, only manifest")
	storeFlag := flags.String("store", "index", "Location of the chunk store")
	offsetFlag := flags.Int64("offset", 0, "Start reading file at given offset")
	exactFlag := flags.Bool("exact", false, "Ignore the NTFS bitmap, i.e. include unused blocks")
	minSizeFlag := flags.String("minsize", fmt.Sprintf("%d", dedupFileSizeMinBytes), "Minimum file size to consider for deduping")

	flags.Parse(args)

	if flags.NArg() < 2 {
		usage()
	}

	debug = *debugFlag
	offset := *offsetFlag
	exact := *exactFlag
	minSize, err := convertToBytes(*minSizeFlag)
	if err != nil {
		exit(2, "Invalid min size value: " + err.Error())
	}

	file := flags.Arg(0)
	manifest := flags.Arg(1)

	var store chunkStore
	if *noWriteFlag {
		store = NewDummyChunkStore()
	} else {
		store = NewFileChunkStore(*storeFlag)
	}

	// Go index!
	if err := index(file, store, manifest, offset, exact, minSize); err != nil {
		exit(2, "Cannot index file: " + string(err.Error()))
	}
}

func mapCommand(args []string) {
	flags := flag.NewFlagSet("map", flag.ExitOnError)
	debugFlag := flags.Bool("debug", debug, "Enable debug mode")
	storeFlag := flags.String("store", "index", "Location of the chunk store")

	flags.Parse(args)

	if flags.NArg() < 1 {
		usage()
	}

	debug = *debugFlag
	filename := flags.Arg(0)

	store := NewFileChunkStore(*storeFlag)

	if err := mapDevice(filename, store); err != nil {
		exit(2, "Cannot map drive file: " + string(err.Error()))
	}
}

func exportCommand(args []string) {
	flags := flag.NewFlagSet("export", flag.ExitOnError)
	debugFlag := flags.Bool("debug", debug, "Enable debug mode")
	storeFlag := flags.String("store", "index", "Location of the chunk store")

	flags.Parse(args)

	if flags.NArg() < 2 {
		usage()
	}

	debug = *debugFlag
	manifest := flags.Arg(0)
	outfile := flags.Arg(1)

	store := NewFileChunkStore(*storeFlag)

	if err := export(manifest, store, outfile); err != nil {
		exit(2, "Cannot export file: " + string(err.Error()))
	}
}

func printCommand(args []string) {
	flags := flag.NewFlagSet("print", flag.ExitOnError)
	debugFlag := flags.Bool("debug", debug, "Enable debug mode")

	flags.Parse(args)

	if flags.NArg() < 1 {
		usage()
	}

	debug = *debugFlag
	manifest := flags.Arg(0)

	m, err := NewManifestFromFile(manifest)
	if err != nil {
		exit(2, "Cannot read manifest: " + string(err.Error()))
	}

	m.Print()
}

func statCommand(args []string) {
	flags := flag.NewFlagSet("stat", flag.ExitOnError)
	debugFlag := flags.Bool("debug", debug, "Enable debug mode")

	flags.Parse(args)

	if flags.NArg() < 1 {
		usage()
	}

	debug = *debugFlag
	manifests := flags.Args()

	if err := printStats(manifests); err != nil {
		exit(2, "Cannot create manifest stats: " + string(err.Error()))
	}
}