VERSION=0.1.0-alpha

help:
	@echo "Build:"
	@echo "  make all   - Build all deliverables"
	@echo "  make cmd   - Build the CLI tool"
	@echo "  make clean - Clean build folder"

all: clean proto cmd

clean:
	@echo == Cleaning ==
	rm -rf build
	@echo

proto:
	@echo == Generating protobuf code ==
	protoc --go_out=. pb/*.proto
	@echo

cmd: proto
	@echo == Building CLI ==
	mkdir -p build/cmd
	go build \
		-o build/fsdup \
		-ldflags \
		"-X main.buildversion=${VERSION} -X main.buildcommit=$(shell git rev-parse --short HEAD) -X main.builddate=$(shell date +%s)" \
		cmd/fsdup/main.go
	@echo
	@echo "--> fsdup CLI built at build/fsdup"
	@echo
