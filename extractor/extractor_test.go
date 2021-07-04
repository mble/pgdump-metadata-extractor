package extractor_test

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/mble/pgdump-metadata-extractor/extractor"
	"github.com/mble/pgdump-metadata-extractor/metadata"
)

func TestCfgValidate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc   string
		config extractor.Cfg
		err    error
	}{
		{
			desc: "no filename or stdin set",
			config: extractor.Cfg{
				FileName: "",
				Stdin:    false,
			},
			err: extractor.ErrInvalidConfig,
		},
		{
			desc: "filename and stdin set",
			config: extractor.Cfg{
				FileName: "latest.dump",
				Stdin:    true,
			},
			err: extractor.ErrInvalidConfig,
		},
		{
			desc: "filename and no stdin",
			config: extractor.Cfg{
				FileName: "latest.dump",
				Stdin:    false,
			},
			err: nil,
		},
		{
			desc: "stdin and no filename",
			config: extractor.Cfg{
				FileName: "",
				Stdin:    true,
			},
			err: nil,
		},
	}
	for _, tC := range testCases {
		tC := tC

		t.Run(tC.desc, func(t *testing.T) {
			t.Parallel()

			res := tC.config.Validate()

			if tC.err != nil {
				exp := tC.err

				if !errors.Is(res, exp) {
					t.Errorf("expected=%v, got=%v", exp, res)
				}

				return
			}

			if res != nil {
				t.Errorf("expected not err, got=%v", res)
			}
		})
	}
}

func TestRunFile(t *testing.T) {
	t.Parallel()

	cfg := extractor.Cfg{FileName: "../testdata/min.dump", Stdin: false}
	fd, _ := os.Open(cfg.FileName)

	if _, err := extractor.Run(fd); err != nil {
		t.Error(err)
	}
}

func TestRunFileErr(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc     string
		filename string
		err      error
	}{
		{
			desc:     "empty file",
			filename: "../testdata/empty.dump",
			err:      io.EOF,
		},
		{
			desc:     "invalid dump",
			filename: "../testdata/not_a.dump",
			err:      metadata.ErrNotADump,
		},
	}

	for _, tC := range testCases {
		tC := tC

		t.Run(tC.desc, func(t *testing.T) {
			t.Parallel()

			cfg := extractor.Cfg{FileName: tC.filename, Stdin: false}
			fd, _ := os.Open(cfg.FileName)

			_, err := extractor.Run(fd)
			if !errors.Is(err, tC.err) {
				t.Errorf("expected=%v, got=%v", tC.err, err)
			}
		})
	}
}
