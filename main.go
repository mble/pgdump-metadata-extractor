package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/mble/pgdump-metadata-extractor/extractor"
)

func run(cfg extractor.Cfg) error {
	var (
		fd  *os.File
		err error
	)

	switch {
	case cfg.Stdin:
		fd = os.Stdin
	case cfg.FileName != "":
		fd, err = os.Open(cfg.FileName)
		if err != nil {
			err = fmt.Errorf("err opening file: %w", err)
			return err
		}
	}

	defer func() {
		if fd != nil && fd != os.Stdin {
			_ = fd.Close()
		}
	}()

	json, err := extractor.Run(fd)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", json)

	return nil
}

func main() {
	cfg := extractor.Cfg{}
	flag.StringVar(&cfg.FileName, "filename", "", "dump to read metadata of")
	flag.BoolVar(&cfg.Stdin, "stdin", false, "configure to read from stdin")
	flag.Parse()

	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}

	err := run(cfg)
	if err != nil {
		log.Fatal(err)
	}
}
