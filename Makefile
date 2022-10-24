build:
	go build ./...
	go vet ./...
	GOARCH=386 go vet ./...
	staticcheck ./...
	./gendoc.sh

fmt:
	go fmt ./...
	gofmt -w -s *.go cmd/bstore/*.go

test:
	go test -shuffle=on -coverprofile cover.out
	go tool cover -html=cover.out -o cover.html

benchmark:
	go test -bench .

fuzz:
	go test -fuzz .
