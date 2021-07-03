package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/mble/pgdump-metadata-extractor/metadata"
)

type Cfg struct {
	fileName         string
	stdin            bool
	enableCPUProfile bool
	enableMemProfile bool
}

func run(cfg Cfg) error {
	var (
		buf *os.File
		err error
	)

	switch {
	case cfg.stdin:
		buf = os.Stdin
	case cfg.fileName != "":
		buf, err = os.Open(cfg.fileName)
		if err != nil {
			err = fmt.Errorf("err opening file: %w", err)
			return err
		}
	}

	defer func() {
		if err = buf.Close(); err != nil {
			log.Fatalf("err closing file: %v", err)
		}
	}()

	data, err := metadata.NewMetadata(buf)
	if err != nil {
		err = fmt.Errorf("err reading metadata: %w", err)

		return err
	}

	json, err := json.Marshal(data)
	if err != nil {
		err = fmt.Errorf("err dumping JSON: %w", err)

		return err
	}

	fmt.Printf("%s", json)

	return nil
}

func main() {
	cfg := Cfg{}
	flag.StringVar(&cfg.fileName, "filename", "", "dump to read metadata of")
	flag.BoolVar(&cfg.stdin, "stdin", false, "configure to read from stdin")
	flag.BoolVar(&cfg.enableCPUProfile, "profile-cpu", false, "enable cpu profile")
	flag.BoolVar(&cfg.enableMemProfile, "profile-mem", false, "enable memory profile")
	flag.Parse()

	if cfg.fileName == "" && !cfg.stdin {
		log.Fatal("file not specified and stdin mode not enabled")
	}

	if cfg.fileName != "" && cfg.stdin {
		log.Fatal("can't provide file and read from stdin")
	}

	if cfg.enableCPUProfile {
		f, err := os.Create("cpu.prof")
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if err := run(cfg); err != nil {
		log.Fatal(err)
	}

	if cfg.enableMemProfile {
		f, err := os.Create("mem.prof")
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
