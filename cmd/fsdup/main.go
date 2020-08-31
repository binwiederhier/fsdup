package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/ncw/swift"
	"github.com/sirupsen/logrus"
	"heckel.io/fsdup"
	"io/ioutil"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// TODO [HIGH] "map": Make caching efficient
// TODO [MED] Add maxSliceSize to fixed size chunker (= input size), allow adding multiple slices to chunk
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
	case "server":
		serverCommand(args)
	case "upload":
		uploadCommand(args)
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
	fmt.Println("  index [-nowrite] [-store STORE] [-offset OFFSET] [-minsize MINSIZE] [-exact] [-nofile] INFILE MANIFEST")
	fmt.Println("  import [-store STORE] INFILE MANIFEST")
	fmt.Println("  export [-store STORE] MANIFEST OUTFILE")
	fmt.Println("  upload [-server ADDR:PORT] INFILE MANIFEST")
	fmt.Println("  map [-store STORE] [-cache CACHE] MANIFEST OUTFILE")
	fmt.Println("  print [disk|chunks] MANIFEST")
	fmt.Println("  server [-store STORE] [ADDR]:PORT")
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
	metaFlag := flags.String("meta", "", "Location of the metadata store")
	offsetFlag := flags.Int64("offset", 0, "Start reading file at given offset")
	exactFlag := flags.Bool("exact", false, "Ignore the NTFS bitmap, i.e. include unused blocks")
	noFileFlag := flags.Bool("nofile", false, "Don't do NTFS FILE deduping, just do gaps and unused space")
	minSizeFlag := flags.String("minsize", fmt.Sprintf("%d", fsdup.DefaultDedupFileSizeMinBytes), "Minimum file size to consider for deduping")
	maxChunkSizeFlag := flags.String("maxchunksize", fmt.Sprintf("%d", fsdup.DefaultChunkSizeMaxBytes), "Maximum size per chunk")
	writeConcurrencyFlag := flags.Int("writeconcurrency", 20, "Number of concurrent write requests against the store")

	flags.Parse(args)

	if flags.NArg() < 2 {
		usage()
	}

	if *debugFlag {
		fsdup.Debug = *debugFlag
	}

	offset := *offsetFlag
	exact := *exactFlag
	noFile := *noFileFlag
	minSize, err := convertToBytes(*minSizeFlag)
	if err != nil {
		exit(2, "Invalid min size value: " + err.Error())
	}

	chunkMaxSize, err := convertToBytes(*maxChunkSizeFlag)
	if err != nil {
		exit(2, "Invalid max chunk size value: " + err.Error())
	}
	writeConcurrency := int64(*writeConcurrencyFlag)

	file := flags.Arg(0)
	manifestId := flags.Arg(1)

	var store fsdup.ChunkStore
	if *noWriteFlag {
		store = fsdup.NewDummyChunkStore()
	} else {
		store, err = createChunkStore(*storeFlag)
		if err != nil {
			exit(2, "Invalid syntax: " + string(err.Error()))
		}
	}

	metaStore, err := createMetaStore(*metaFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	// Go index!
	if err := fsdup.Index(file, store, metaStore, manifestId, offset, exact, noFile, minSize, chunkMaxSize, writeConcurrency); err != nil {
		exit(2, "Cannot index file: " + string(err.Error()))
	}
}

func mapCommand(args []string) {
	flags := flag.NewFlagSet("map", flag.ExitOnError)
	debugFlag := flags.Bool("debug", fsdup.Debug, "Enable debug mode")
	storeFlag := flags.String("store", "index", "Location of the chunk store")
	metaFlag := flags.String("meta", "", "Location of the metadata store")
	cacheFlag := flags.String("cache", "cache", "Location of the chunk cache")
	fingerprintFlag := flags.String("fingerprint", "", "Location of the fingerprint file")
	deviceFlag := flags.String("device", "", "Name of the NBD device (chosen automatically if not set)")

	flags.Parse(args)

	if flags.NArg() < 2 {
		usage()
	}

	if *debugFlag {
		fsdup.Debug = *debugFlag
	}

	manifestId := flags.Arg(0)
	targetFile := flags.Arg(1)

	store, err := createChunkStore(*storeFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	metaStore, err := createMetaStore(*metaFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	cache := fsdup.NewFileChunkStore(*cacheFlag)

	// Set up copy-on-demand device
	if *deviceFlag == "" {
		*deviceFlag, err = findNextNbdDevice()
		if err != nil {
			exit(2, "Cannot find NBD device: " + err.Error())
		}


	}

	errorChan := make(chan error)
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)

	device, err := fsdup.Map(manifestId, store, metaStore, cache, targetFile, *fingerprintFlag, *deviceFlag)
	if err != nil {
		exit(2, "Cannot map drive file: " + string(err.Error()))
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func(errorChan chan<- error) {
		if err := device.Connect(); err != nil {
			errorChan <- fmt.Errorf("Buse device stopped with error: %s", err)
		} else {
			logrus.Info("Buse device stopped gracefully.")
		}
	}(errorChan)

	go outputStatus(ctx, device)

	// Block until SIGINT (or error) is received
	logrus.Info("Device " + *deviceFlag + " is ready to be used. Stop via SIGINT/Ctrl-C.")

	select {
	case <-sig:
		break
	case err := <-errorChan:
		logrus.Error(err)
		break
	}

	// Cancel and disconnect
	logrus.Info("Stopping device ...")
	cancel()
	device.Disconnect()

	logrus.Info("Sleeping 10 sec ...")
	time.Sleep(10 * time.Second)

	logrus.Info("Done")
}

func exportCommand(args []string) {
	flags := flag.NewFlagSet("export", flag.ExitOnError)
	debugFlag := flags.Bool("debug", fsdup.Debug, "Enable debug mode")
	storeFlag := flags.String("store", "index", "Location of the chunk store")
	metaFlag := flags.String("meta", "", "Location of the metadata store")

	flags.Parse(args)

	if flags.NArg() < 2 {
		usage()
	}

	if *debugFlag {
		fsdup.Debug = *debugFlag
	}

	manifestId := flags.Arg(0)
	outputFile := flags.Arg(1)

	store, err := createChunkStore(*storeFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	metaStore, err := createMetaStore(*metaFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	if err := fsdup.Export(manifestId, store, metaStore, outputFile); err != nil {
		exit(2, "Cannot export file: " + string(err.Error()))
	}
}

func importCommand(args []string) {
	flags := flag.NewFlagSet("import", flag.ExitOnError)
	debugFlag := flags.Bool("debug", fsdup.Debug, "Enable debug mode")
	storeFlag := flags.String("store", "index", "Location of the chunk store")
	metaFlag := flags.String("meta", "", "Location of the metadata store")

	flags.Parse(args)

	if flags.NArg() < 2 {
		usage()
	}

	if *debugFlag {
		fsdup.Debug = *debugFlag
	}

	inputFile := flags.Arg(0)
	manifestId := flags.Arg(1)

	store, err := createChunkStore(*storeFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	metaStore, err := createMetaStore(*metaFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	if err := fsdup.Import(manifestId, store, metaStore, inputFile); err != nil {
		exit(2, "Cannot import file: " + string(err.Error()))
	}
}

func printCommand(args []string) {
	flags := flag.NewFlagSet("print", flag.ExitOnError)
	debugFlag := flags.Bool("debug", fsdup.Debug, "Enable debug mode")
	metaFlag := flags.String("meta", "", "Location of the metadata store")

	flags.Parse(args)

	if flags.NArg() < 1 {
		usage()
	}

	if *debugFlag {
		fsdup.Debug = *debugFlag
	}

	metaStore, err := createMetaStore(*metaFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	var what string
	var manifestId string

	if flags.NArg() == 1 {
		what = "disk"
		manifestId = flags.Arg(0)
	} else {
		what = flags.Arg(0)
		manifestId = flags.Arg(1)
	}

	manifest, err := metaStore.ReadManifest(manifestId)
	if err != nil {
		exit(2, "Cannot read manifest: " + string(err.Error()))
	}

	switch what {
	case "disk":
		manifest.PrintDisk()
	case "chunks":
		if err := manifest.PrintChunks(); err != nil {
			exit(2, "Cannot print chunks: " + string(err.Error()))
		}
	default:
		usage()
	}
}

func statCommand(args []string) {
	flags := flag.NewFlagSet("stat", flag.ExitOnError)
	debugFlag := flags.Bool("debug", fsdup.Debug, "Enable debug mode")
	verboseFlag := flags.Bool("verbose", false, "Enable verbose mode")
	metaFlag := flags.String("meta", "", "Location of the metadata store")

	flags.Parse(args)

	if flags.NArg() < 1 {
		usage()
	}

	if *debugFlag {
		fsdup.Debug = *debugFlag
	}

	metaStore, err := createMetaStore(*metaFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	manifestIds := flags.Args()

	if err := fsdup.Stat(manifestIds, metaStore, *verboseFlag); err != nil {
		exit(2, "Cannot create manifest stats: " + string(err.Error()))
	}
}

func serverCommand(args []string) {
	flags := flag.NewFlagSet("server", flag.ExitOnError)
	debugFlag := flags.Bool("debug", fsdup.Debug, "Enable debug mode")
	storeFlag := flags.String("store", "index", "Location of the chunk store")
	metaFlag := flags.String("meta", "", "Location of the metadata store")

	flags.Parse(args)

	if flags.NArg() < 1 {
		usage()
	}

	if *debugFlag {
		fsdup.Debug = *debugFlag
	}

	store, err := createChunkStore(*storeFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	metaStore, err := createMetaStore(*metaFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	listenAddr := flags.Arg(0)
	if err := fsdup.ListenAndServe(listenAddr, store, metaStore); err != nil {
		exit(1, err.Error())
	}
}

func uploadCommand(args []string) {
	flags := flag.NewFlagSet("upload", flag.ExitOnError)
	debugFlag := flags.Bool("debug", fsdup.Debug, "Enable debug mode")
	serverFlag := flags.String("server", ":9991", "Server address")
	metaFlag := flags.String("meta", "", "Location of the metadata store")
	flags.Parse(args)

	if flags.NArg() < 2 {
		usage()
	}

	if *debugFlag {
		fsdup.Debug = *debugFlag
	}

	metaStore, err := createMetaStore(*metaFlag)
	if err != nil {
		exit(2, "Invalid syntax: " + string(err.Error()))
	}

	inputFile := flags.Arg(0)
	manifestId := flags.Arg(1)

	if err := fsdup.Upload(manifestId, metaStore, inputFile, *serverFlag); err != nil {
		exit(2, "Cannot upload chunks for file: " + string(err.Error()))
	}
}

func createChunkStore(spec string) (fsdup.ChunkStore, error) {
	if regexp.MustCompile(`^(ceph|swift|gcloud|remote):`).MatchString(spec) {
		uri, err := url.ParseRequestURI(spec)
		if err != nil {
			return nil, err
		}

		if uri.Scheme == "ceph" {
			return createCephChunkStore(uri)
		} else if uri.Scheme == "swift" {
			return createSwiftChunkStore(uri)
		} else if uri.Scheme == "gcloud" {
			return createGcloudChunkStore(uri)
		} else if uri.Scheme == "remote" {
			return createRemoteChunkStore(uri)
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

func createSwiftChunkStore(uri *url.URL) (fsdup.ChunkStore, error) {
	connection := &swift.Connection{}

	container := uri.Query().Get("container")
	if container == "" {
		return nil, errors.New("invalid syntax for swift store type, container parameter is required")
	}

	err := connection.ApplyEnvironment()
	if err != nil {
		return nil, err
	}

	// TODO provide way to override environment variables

	return fsdup.NewSwiftStore(connection, container), nil
}

func createGcloudChunkStore(uri *url.URL) (fsdup.ChunkStore, error) {
	project := uri.Query().Get("project")
	if project == "" {
		return nil, errors.New("invalid syntax for gcloud store type, project parameter is required")
	}

	bucket := uri.Query().Get("bucket")
	if bucket == "" {
		return nil, errors.New("invalid syntax for gcloud store type, bucket parameter is required")
	}

	return fsdup.NewGcloudStore(project, bucket), nil
}

func createRemoteChunkStore(uri *url.URL) (fsdup.ChunkStore, error) {
	return fsdup.NewRemoteChunkStore(uri.Opaque), nil
}

func createMetaStore(spec string) (fsdup.MetaStore, error) {
	if regexp.MustCompile(`^(remote|mysql):`).MatchString(spec) {
		uri, err := url.ParseRequestURI(spec)
		if err != nil {
			return nil, err
		}

		if uri.Scheme == "remote" {
			return createRemoteMetaStore(uri)
		} else if uri.Scheme == "mysql" {
			return createMysqlMetaStore(uri)
		}

		return nil, errors.New("meta store type not supported")
	}

	return createFileMetaStore()
}

func createFileMetaStore() (fsdup.MetaStore, error) {
	return fsdup.NewFileMetaStore(), nil
}

func createRemoteMetaStore(uri *url.URL) (fsdup.MetaStore, error) {
	return fsdup.NewRemoteMetaStore(uri.Opaque), nil
}

func createMysqlMetaStore(uri *url.URL) (fsdup.MetaStore, error) {
	return fsdup.NewMysqlMetaStore(uri.Opaque)
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

func findNextNbdDevice() (string, error) {
	for i := 0; i < 256; i++ {
		sizeFile := fmt.Sprintf("/sys/class/block/nbd%d/size", i)

		if _, err := os.Stat(sizeFile); err == nil {
			b, err := ioutil.ReadFile(sizeFile)
			if err != nil {
				return "", err
			}

			if strings.Trim(string(b), "\n") == "0" {
				return fmt.Sprintf("/dev/nbd%d", i), nil
			}
		}
	}

	return "", errors.New("cannot find free nbd device, driver not loaded?")
}
