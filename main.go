package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/mble/pgdump-metadata-extractor/extractor"
)

func main() {
	var (
		fd       *os.File
		err      error
		exitCode int
	)

	defer func() {
		os.Exit(exitCode)
	}()

	cfg := extractor.Cfg{}
	flag.StringVar(&cfg.FileName, "filename", "", "dump to read metadata of")
	flag.BoolVar(&cfg.Stdin, "stdin", false, "configure to read from stdin")
	flag.Parse()

	if err := cfg.Validate(); err != nil {
		log.Print(err)

		exitCode = 1

		return
	}

	switch {
	case cfg.Stdin:
		fd = os.Stdin
	case cfg.FileName != "":
		fd, err = os.Open(cfg.FileName)
		if err != nil {
			err = fmt.Errorf("err opening file: %w", err)

			log.Print(err)

			exitCode = 1

			return
		}
	}

	defer func() {
		fd.Close()
	}()

	json, err := extractor.Run(fd)
	if err != nil {
		log.Print(err)

		exitCode = 1

		return
	}

	fmt.Printf("%s", json)
}
