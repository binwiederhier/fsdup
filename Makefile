help:
	@echo "Build:"
	@echo "  make all   - Build all deliverables"
	@echo "  make cmd   - Build the natter CLI tool & Go library"
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
	@echo == Building natter CLI ==
	mkdir -p build/cmd
	go build -o build/fsdup cmd/fsdup/main.go
	@echo
	@echo "--> fsdup CLI built at build/fsdup"
	@echo
