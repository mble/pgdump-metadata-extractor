package metadata

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
)

var ErrNotADump = errors.New("magic bytes not detected, not a dump?")

// Metadata represents the metadata about the dump.
type Metadata struct {
	// Magic is the magic byte string.
	Magic string `json:"magic"`
	// Format is the format of the dump.
	Format string `json:"format"`
	// PGDumpVersion is the version of pg_dump used to create the dump.
	PGDumpVersion string `json:"pgDumpVersion"`
	// RemoteVersion is the version of the PostgreSQL cluster dumped.
	RemoteVersion string `json:"remoteVersion"`
	// DatabaseName is the name of the database dumped.
	DatabaseName string `json:"database"`
	// TimeYear forms the year part of the creation timestamp.
	TimeYear int `json:"timeYear"`
	// TimeMonth forms the month part of the creation timestamp.
	TimeMonth int `json:"timeMonth"`
	// TimeDay forms the day part of the creation timestamp.
	TimeDay int `json:"timeDay"`
	// TimeHour forms the hours part of the creation timestamp.
	TimeHour int `json:"timeHour"`
	// TimeMin forms the minutes part of the creation timestamp.
	TimeMin int `json:"timeMin"`
	// TimeSec forms the seconds part of the creation timestamp.
	TimeSec int `json:"timeSec"`
	// TimeIsDST is a flag to determine if the DST applies to the timestamp.
	TimeIsDST int `json:"timeIsDst"`
	// Compression represents if compression is enabled on the dump.
	Compression int `json:"compression"`
	// TOCCount is the count of TOC centires in the dump.
	TOCCount int `json:"toccount"`
	// IntSize is the int size, in bytes.
	IntSize uint8 `json:"intsize"`
	// VRev is the revision number of the archive format.
	VRev uint8 `json:"vrev"`
	// VMin is the minor version of the archive format.
	VMin uint8 `json:"vmin"`
	// VMain is the major version of the archive format.
	VMain uint8 `json:"vmain"`
	// OffSize is the offset size, in bytes.
	OffSize uint8 `json:"offsize"`
}

// ReadInt reads bytes from reader and operates in reverse byte order, returning an int.
func (m *Metadata) ReadInt(reader io.Reader) int {
	val := 0
	byteLength := 8
	sign := ReadExactInt(reader, 1)
	buf := make([]byte, m.IntSize)

	if _, err := reader.Read(buf); err != nil {
		log.Fatalf("err reading int: %v", err)
	}

	for len(buf) > 0 {
		v := buf[len(buf)-1]
		buf = buf[:len(buf)-1]
		val = (val << byteLength) + int(v)
	}

	if sign > 0 {
		val = -val
	}

	return val
}

// ReadString reads bytes from the reader, returning a string.
func (m *Metadata) ReadString(reader io.Reader) string {
	val := ""

	if length := m.ReadInt(reader); length > 0 {
		buf := make([]byte, length)
		if _, err := reader.Read(buf); err != nil {
			log.Fatalf("err reading string: %v", err)
		}

		val = string(buf)
	}

	return val
}

// ToJSON returns a JSON representation of the metadata.
func (m *Metadata) ToJSON() ([]byte, error) {
	out, err := json.Marshal(m)
	if err != nil {
		err = fmt.Errorf("err dumping JSON: %w", err)

		return []byte{}, err
	}

	return out, nil
}

// NewMetadata reads from reader, parsing out the pg_dump archive header format
// into a Metadata struct.
func NewMetadata(reader io.Reader) (Metadata, error) {
	formats := []string{"UNKNOWN", "CUSTOM", "FILE", "TAR", "NULL", "DIRECTORY"}
	yearStart := 1900
	metadata := Metadata{}

	r := bufio.NewReader(reader)
	magicBytes := 5

	magicString, err := ReadExactString(r, magicBytes)
	if err != nil {
		return metadata, fmt.Errorf("err reading magic bytes: %w", err)
	}

	metadata.Magic = magicString

	if metadata.Magic != "PGDMP" {
		err := fmt.Errorf("%w, expected=PGDMP, got=%s not a dump?", ErrNotADump, metadata.Magic)

		return metadata, err
	}

	metadata.VMain = ReadExactInt(r, 1)
	metadata.VMin = ReadExactInt(r, 1)
	metadata.VRev = ReadExactInt(r, 1)
	metadata.IntSize = ReadExactInt(r, 1)
	metadata.OffSize = ReadExactInt(r, 1)
	metadata.Format = formats[ReadExactInt(r, 1)]
	metadata.Compression = metadata.ReadInt(r)
	metadata.TimeSec = metadata.ReadInt(r)
	metadata.TimeMin = metadata.ReadInt(r)
	metadata.TimeHour = metadata.ReadInt(r)
	metadata.TimeDay = metadata.ReadInt(r)
	metadata.TimeMonth = metadata.ReadInt(r)
	metadata.TimeYear = yearStart + metadata.ReadInt(r)
	metadata.TimeIsDST = metadata.ReadInt(r)
	metadata.DatabaseName = metadata.ReadString(r)
	metadata.RemoteVersion = metadata.ReadString(r)
	metadata.PGDumpVersion = metadata.ReadString(r)
	metadata.TOCCount = metadata.ReadInt(r)

	return metadata, nil
}

// ReadExactString reads a string from the reader, numBytes from current position.
func ReadExactString(reader io.Reader, numBytes int) (string, error) {
	buf := make([]byte, numBytes)

	n, err := reader.Read(buf)
	if err != nil {
		return "", fmt.Errorf("err reading exact string: %w", err)
	}

	return string(buf[0:n]), nil
}

// ReadExactInt reads an int from the reader, numBytes from current position.
func ReadExactInt(reader io.Reader, numBytes int) uint8 {
	buf := make([]byte, numBytes)

	n, err := reader.Read(buf)
	if err != nil {
		log.Fatalf("err reading exact int: %v", err)
	}

	return buf[0:n][0]
}
