TARGET := bin/pgdump-metadata-extractor

build:
	CGO_ENABLED=0 go build -o $(TARGET)
