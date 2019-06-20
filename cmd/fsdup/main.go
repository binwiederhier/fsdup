package main

import (
	"errors"
	"flag"
	"fmt"
	"heckel.io/fsdup"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// TODO [HIGH] "map": Make caching efficient
// TODO [LOW] Find zeros in gaps, mark as sparse
// TODO [LOW] Find zeros in FILE runs, mark as sparse
// TODO [LOW] rename "size" to "length"
// TODO [LOW] chunkPart.to|from -> offset|length
// TODO [LOW] different debug levels -d -dd -ddd -q

var (
	buildversion = "0.0.0-DEV"
	builddate = "0"
	buildcommit = "dev version"
)

func main() {
	versionFlag := flag.Bool("version", false, "Displays the version of this program")
	debugFlag := flag.Bool("debug", false, "Enable debug information")
	quietFlag := flag.Bool("quiet", false, "Do not print status information")

	flag.Parse()

	fsdup.Debug = *debugFlag
	fsdup.Quiet = *quietFlag

	if *versionFlag {
		displayVersion()
	}

	if flag.NArg() < 2 {
		usage()
	}

	command := flag.Args()[0]
	args := flag.Args()[1:]

	switch command {
	case "index":
		indexCommand(args)
	case "map":
		mapCommand(args)
	case "export":
		exportCommand(args)
	case "import":
		importCommand(args)
	case "print":
		printCommand(args)
	case "stat":
		statCommand(args)
	default:
		usage()
	}
}

func exit(code int, message string) {
	fmt.Println(message)
	os.Exit(code)
}

func usage() {
	fmt.Println("Syntax:")
	fmt.Println("  fsdup [-quiet] [-debug] COMMAND [options ...]")
	fmt.Println("")
	fmt.Println("Commands:")
	fmt.Println("  index [-nowrite] [-store STORE] [-offset OFFSET] [-minsize MINSIZE] [-exact] INFILE MANIFEST")
	fmt.Println("  import [-store STORE] INFILE MANIFEST")
	fmt.Println("  export [-store STORE] MANIFEST OUTFILE")
	fmt.Println("  map [-store STORE] [-cache CACHE] MANIFEST OUTFILE")
	fmt.Println("  print MANIFEST")
	fmt.Println("  stat MANIFEST...")

	os.Exit(1)
}

func displayVersion() {
	unixtime, _ := strconv.Atoi(builddate)
	datestr := time.Unix(int64(unixtime), 0).Format("01/02/06 15:04")

	fmt.Printf("fsdup version %s, built at %s from %s\n", buildversion, datestr, buildcommit)
	fmt.Printf("Distributed under the Apache License 2.0, see LICENSE file for details\n")
	fmt.Printf("Copyright (C) 2019 Philipp Heckel\n")

	os.Exit(1)
}

func indexCommand(args []string) {
	flags := flag.NewFlagSet("index", flag.ExitOnError)
	debugFlag := flags.Bool("debug", fsdup.Debug, "Enable debug mode")
	noWriteFlag := flags.Bool("nowrite", false, "Do not write chunk data, only manifest")
	storeFlag := flags.String("store", "index", "Location of the chunk store")
	offsetFlag := flags.Int64("offset", 0, "Start reading file at given offset")
	exactFlag := flags.Bool("exact", false, "Ignore the NTFS bitmap, i.e. include unused blocks")
	minSizeFlag := flags.String("minsize", fmt.Sprintf("%d", fsdup.DefaultDedupFileSizeMinBytes), "Minimum file size to consider for deduping")

	flags.Parse(args)

	if flags.NArg() < 2 {
		usage()
	}

	if *debugFlag {
		fsdup.Debug = *debugFlag
	}

	offset := *offsetFlag
	exact := *exactFlag
	minSize, err := convertToBytes(*minSizeFlag)
	if err != nil {
		exit(2, "Invalid min size value: " + err.Error())
	}

	file := flags.Arg(0)
	manifest := flags.Arg(1)

	var store fsdup.ChunkStore
	if *noWriteFlag {
		store = fsdup.NewDummyChunkStore()
	} else {
		store, err = createChunkStore(*storeFlag)
		if err != nil {
			exit(2, "Invalid syntax: " + string(err.Error()))
		}
	}

	// Go index!
	if err := fsdup.Index(file, store, manifest, offset, exact, minSize); err != nil {
		exit(2, "Cannot index file: " + string(err.Error()))
	}
}

func mapCommand(args []string) {
	flags := flag.NewFlagSet("map", flag.ExitOnError)
	debugFlag := flags.Bool("debug", fsdup.Debug, "Enable debug mode")
	storeFlag := flags.String("store", "index", "Location of the chunk store")
	cacheFlag := flags.String("cache", "cache", "Location of the chunk cache")

	flags.Parse(args)

	if flags.NArg() < 2 {
		usage()
	}

	if *debugFlag {
		fsdup.Debug = *debugFlag
	}

	manifestFile := flags.Arg(0)
	targetFile := flags.Arg(1)

	store, err := createChunkStore(*storeFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	cache := fsdup.NewFileChunkStore(*cacheFlag)

	if err := fsdup.Map(manifestFile, store, cache, targetFile); err != nil {
		exit(2, "Cannot map drive file: " + string(err.Error()))
	}
}

func exportCommand(args []string) {
	flags := flag.NewFlagSet("export", flag.ExitOnError)
	debugFlag := flags.Bool("debug", fsdup.Debug, "Enable debug mode")
	storeFlag := flags.String("store", "index", "Location of the chunk store")

	flags.Parse(args)

	if flags.NArg() < 2 {
		usage()
	}

	if *debugFlag {
		fsdup.Debug = *debugFlag
	}

	manifest := flags.Arg(0)
	outfile := flags.Arg(1)

	store, err := createChunkStore(*storeFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	if err := fsdup.Export(manifest, store, outfile); err != nil {
		exit(2, "Cannot export file: " + string(err.Error()))
	}
}

func importCommand(args []string) {
	flags := flag.NewFlagSet("import", flag.ExitOnError)
	debugFlag := flags.Bool("debug", fsdup.Debug, "Enable debug mode")
	storeFlag := flags.String("store", "index", "Location of the chunk store")

	flags.Parse(args)

	if flags.NArg() < 2 {
		usage()
	}

	if *debugFlag {
		fsdup.Debug = *debugFlag
	}

	infile := flags.Arg(0)
	manifest := flags.Arg(1)

	store, err := createChunkStore(*storeFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	if err := fsdup.Import(manifest, store, infile); err != nil {
		exit(2, "Cannot import file: " + string(err.Error()))
	}
}

func printCommand(args []string) {
	flags := flag.NewFlagSet("print", flag.ExitOnError)
	debugFlag := flags.Bool("debug", fsdup.Debug, "Enable debug mode")

	flags.Parse(args)

	if flags.NArg() < 1 {
		usage()
	}

	if *debugFlag {
		fsdup.Debug = *debugFlag
	}

	manifestFile := flags.Arg(0)

	manifest, err := fsdup.NewManifestFromFile(manifestFile)
	if err != nil {
		exit(2, "Cannot read manifest: " + string(err.Error()))
	}

	manifest.Print()
}

func statCommand(args []string) {
	flags := flag.NewFlagSet("stat", flag.ExitOnError)
	debugFlag := flags.Bool("debug", fsdup.Debug, "Enable debug mode")
	verboseFlag := flags.Bool("verbose", false, "Enable verbose mode")

	flags.Parse(args)

	if flags.NArg() < 1 {
		usage()
	}

	if *debugFlag {
		fsdup.Debug = *debugFlag
	}

	manifests := flags.Args()

	if err := fsdup.Stat(manifests, *verboseFlag); err != nil {
		exit(2, "Cannot create manifest stats: " + string(err.Error()))
	}
}

func createChunkStore(spec string) (fsdup.ChunkStore, error) {
	if regexp.MustCompile(`^ceph:`).MatchString(spec) {
		uri, err := url.ParseRequestURI(spec)
		if err != nil {
			return nil, err
		}

		if uri.Scheme == "ceph" {
			return createCephChunkStore(uri)
		}

		return nil, errors.New("store type not supported")
	}

	return fsdup.NewFileChunkStore(spec), nil
}

func createCephChunkStore(uri *url.URL) (fsdup.ChunkStore, error) {
	var configFile string
	var pool string

	if uri.Opaque != "" {
		configFile = uri.Opaque
	} else if uri.Path != "" {
		configFile = uri.Path
	} else {
		configFile = "/etc/ceph/ceph.conf"
	}

	if _, err := os.Stat(configFile); err != nil {
		return nil, err
	}

	pool = uri.Query().Get("pool")
	if pool == "" {
		return nil, errors.New("invalid syntax for ceph store type, should be ceph:FILE?pool=POOL")
	}

	compressStr := uri.Query().Get("compress")
	compress := compressStr == "yes" || compressStr == "true"

	return fsdup.NewCephStore(configFile, pool, compress), nil
}

func convertToBytes(s string) (int64, error) {
	r := regexp.MustCompile(`^(\d+)([bBkKmMgGtT])?$`)
	matches := r.FindStringSubmatch(s)

	if matches == nil {
		return 0, errors.New("cannot convert to bytes: " + s)
	}

	value, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, err
	}

	unit := strings.ToLower(matches[2])
	switch unit {
	case "k":
		return int64(value) * (1 << 10), nil
	case "m":
		return int64(value) * (1 << 20), nil
	case "g":
		return int64(value) * (1 << 30), nil
	case "t":
		return int64(value) * (1 << 40), nil
	default:
		return int64(value), nil
	}
}
