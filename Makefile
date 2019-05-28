all: clean
	mkdir -p build
	protoc --go_out=. pb/*.proto
	go build -o build/fsdup

clean:
	rm -rf build
