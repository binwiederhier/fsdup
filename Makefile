all:
	protoc --go_out=. internal/*.proto
	go build
	
