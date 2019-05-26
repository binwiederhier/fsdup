all: clean
	mkdir -p build
	protoc --go_out=. internal/*.proto
	go build -o build/fsdup

clean:
	rm -rf build
