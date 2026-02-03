package extractor_test

import (
	"errors"
	"os"
	"testing"

	"github.com/mble/pgdump-metadata-extractor/extractor"
	"github.com/mble/pgdump-metadata-extractor/metadata"
)

func TestCfgValidate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		err    error
		desc   string
		config extractor.Cfg
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
	fd, err := os.Open(cfg.FileName)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = fd.Close()
	})

	if _, err := extractor.Run(fd); err != nil {
		t.Error(err)
	}
}

func TestRunFileErr(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		err      error
		desc     string
		filename string
	}{
		{
			desc:     "empty file",
			filename: "../testdata/empty.dump",
			err:      metadata.ErrNeedMoreData,
		},
		{
			desc:     "invalid dump",
			filename: "../testdata/not_a.dump",
			err:      metadata.ErrNotADump,
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			t.Parallel()

			cfg := extractor.Cfg{FileName: tC.filename, Stdin: false}
			fd, err := os.Open(cfg.FileName)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = fd.Close()
			})

			_, err = extractor.Run(fd)
			if !errors.Is(err, tC.err) {
				t.Errorf("expected=%v, got=%v", tC.err, err)
			}
		})
	}
}
