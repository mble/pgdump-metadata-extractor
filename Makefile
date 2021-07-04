TARGET := bin/pgdump-metadata-extractor

default: test
.PHONY: default

build:
	CGO_ENABLED=0 go build -o $(TARGET)
.PHONY: build

lint:
	golangci-lint run ./...
.PHONY: lint

test:
	go test -v -race -coverprofile c.out ./...
.PHONY: test

