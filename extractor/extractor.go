package extractor

import (
	"errors"
	"fmt"
	"io"

	"github.com/mble/pgdump-metadata-extractor/metadata"
)

var ErrInvalidConfig = errors.New("invalid config")

// Cfg holds the config for the extractor.
type Cfg struct {
	FileName string
	Stdin    bool
}

// Validate ensures that Cfg struct is valid.
func (c *Cfg) Validate() error {
	if c.FileName == "" && !c.Stdin {
		return fmt.Errorf("%w: file not specified and stdin mode not enabled", ErrInvalidConfig)
	}

	if c.FileName != "" && c.Stdin {
		return fmt.Errorf("%w: can't provide file and read from stdin", ErrInvalidConfig)
	}

	return nil
}

// Run attempts to read metadata from fd byte-by-byte,
// returning JSON or an error.
func Run(fd io.Reader) ([]byte, error) {
	data, err := metadata.NewMetadata(fd)
	if err != nil {
		err = fmt.Errorf("err reading metadata: %w", err)

		return nil, err
	}

	json, err := data.ToJSON()
	if err != nil {
		return nil, err
	}

	return json, nil
}
