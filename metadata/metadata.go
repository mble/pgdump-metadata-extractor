package metadata

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

var ErrNotADump = errors.New("magic bytes not detected, not a dump?")
var ErrNeedMoreData = errors.New("need more data to parse metadata")
var ErrInvalidIntSize = errors.New("invalid int size")
var ErrInvalidOffSize = errors.New("invalid offset size")
var ErrInvalidReadSize = errors.New("invalid read size")
var ErrIntOverflow = errors.New("integer overflow")
var ErrStringTooLarge = errors.New("string length too large")

const maxStringLen = 1 << 20

var (
	maxInt = int(^uint(0) >> 1)
	minInt = -maxInt - 1
)

// formats maps format index to format name for pg_dump archives.
var formats = [...]string{"UNKNOWN", "CUSTOM", "FILE", "TAR", "NULL", "DIRECTORY"}

// Metadata represents the metadata about the dump.
type Metadata struct {
	// Magic is the magic byte string.
	Magic string `json:"magic"`
	// Format is the format of the dump.
	Format string `json:"format"`
	// PGDumpVersion is the version of pg_dump used to create the dump.
	PGDumpVersion *string `json:"pgDumpVersion"`
	// RemoteVersion is the version of the PostgreSQL cluster dumped.
	RemoteVersion *string `json:"remoteVersion"`
	// DatabaseName is the name of the database dumped.
	DatabaseName *string `json:"database"`
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
	// Compression represents if compression is enabled on the dump (format < 1.15).
	Compression int `json:"compression,omitempty"`
	// CompressionSpec is the compression specification string (format >= 1.15).
	CompressionSpec *string `json:"compressionSpec,omitempty"`
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

// ArchiveVersion returns the archive format version as a comparable integer.
// Format: (major << 16) | (minor << 8) | rev
func (m *Metadata) ArchiveVersion() int {
	return (int(m.VMain) << 16) | (int(m.VMin) << 8) | int(m.VRev)
}

// ReadInt reads bytes from reader and operates in reverse byte order, returning an int64.
func (m *Metadata) ReadInt(reader io.Reader) (int64, error) {
	if m.IntSize == 0 || m.IntSize > 8 {
		return 0, fmt.Errorf("%w: intsize=%d", ErrInvalidIntSize, m.IntSize)
	}

	sign, err := ReadExactInt(reader, 1)
	if err != nil {
		return 0, err
	}

	buf := make([]byte, int(m.IntSize))
	if _, err := io.ReadFull(reader, buf); err != nil {
		return 0, mapReadErr(err)
	}

	var val uint64
	for i := int(m.IntSize) - 1; i >= 0; i-- {
		val = (val << 8) + uint64(buf[i])
	}

	if sign > 0 {
		if val > uint64(maxInt)+1 {
			return 0, fmt.Errorf("%w: %d", ErrIntOverflow, val)
		}
		return -int64(val), nil
	}

	if val > uint64(maxInt) {
		return 0, fmt.Errorf("%w: %d", ErrIntOverflow, val)
	}

	return int64(val), nil
}

// ReadString reads bytes from the reader, returning a string pointer.
// A negative length is treated as NULL and returns a nil pointer.
func (m *Metadata) ReadString(reader io.Reader) (*string, error) {
	length, err := m.ReadInt(reader)
	if err != nil {
		return nil, err
	}

	if length < 0 {
		return nil, nil
	}

	if length == 0 {
		empty := ""
		return &empty, nil
	}

	if length > int64(maxStringLen) {
		return nil, fmt.Errorf("%w: %d", ErrStringTooLarge, length)
	}

	if length > int64(maxInt) {
		return nil, fmt.Errorf("%w: %d", ErrIntOverflow, length)
	}

	buf := make([]byte, int(length))
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, mapReadErr(err)
	}

	val := string(buf)
	return &val, nil
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
	const yearStart = 1900
	metadata := Metadata{}

	r := bufio.NewReader(reader)
	magicBytes := 5

	magicString, err := ReadExactString(r, magicBytes)
	if err != nil {
		return metadata, fmt.Errorf("err reading magic bytes: %w", err)
	}

	metadata.Magic = magicString

	if metadata.Magic != "PGDMP" {
		return metadata, fmt.Errorf("%w, expected=PGDMP, got=%s not a dump?", ErrNotADump, metadata.Magic)
	}

	if metadata.VMain, err = ReadExactInt(r, 1); err != nil {
		return metadata, err
	}
	if metadata.VMin, err = ReadExactInt(r, 1); err != nil {
		return metadata, err
	}
	if metadata.VRev, err = ReadExactInt(r, 1); err != nil {
		return metadata, err
	}
	if metadata.IntSize, err = ReadExactInt(r, 1); err != nil {
		return metadata, err
	}
	if metadata.IntSize == 0 || metadata.IntSize > 8 {
		return metadata, fmt.Errorf("%w: intsize=%d", ErrInvalidIntSize, metadata.IntSize)
	}
	if metadata.OffSize, err = ReadExactInt(r, 1); err != nil {
		return metadata, err
	}
	if metadata.OffSize == 0 || metadata.OffSize > 8 {
		return metadata, fmt.Errorf("%w: offsize=%d", ErrInvalidOffSize, metadata.OffSize)
	}

	formatIdx, err := ReadExactInt(r, 1)
	if err != nil {
		return metadata, err
	}
	if int(formatIdx) >= len(formats) {
		return metadata, fmt.Errorf("invalid format index: %d", formatIdx)
	}
	metadata.Format = formats[formatIdx]

	readIntField := func(name string) (int, error) {
		value, readErr := metadata.ReadInt(r)
		if readErr != nil {
			return 0, readErr
		}
		if value > int64(maxInt) || value < int64(minInt) {
			return 0, fmt.Errorf("%w: %s=%d", ErrIntOverflow, name, value)
		}
		return int(value), nil
	}

	// Archive format version 1.15+ (PostgreSQL 14+) changed compression from int to string.
	// Version 1.16+ (PostgreSQL 16+) changed the format again - the compression algorithm
	// is stored as a single byte indicator.
	const versionWithCompressionSpec = (1 << 16) | (15 << 8) // 1.15
	const versionWithNewCompression = (1 << 16) | (16 << 8) // 1.16
	if metadata.ArchiveVersion() >= versionWithNewCompression {
		// Format 1.16+: Read single-byte compression algorithm indicator
		compressionAlgo, readErr := ReadExactInt(r, 1)
		if readErr != nil {
			return metadata, readErr
		}
		metadata.Compression = int(compressionAlgo)
	} else if metadata.ArchiveVersion() >= versionWithCompressionSpec {
		// Format 1.15.x: Compression is a string specification
		if metadata.CompressionSpec, err = metadata.ReadString(r); err != nil {
			return metadata, err
		}
	} else {
		// Older formats use an integer for compression level
		if metadata.Compression, err = readIntField("compression"); err != nil {
			return metadata, err
		}
	}
	if metadata.TimeSec, err = readIntField("timeSec"); err != nil {
		return metadata, err
	}
	if metadata.TimeMin, err = readIntField("timeMin"); err != nil {
		return metadata, err
	}
	if metadata.TimeHour, err = readIntField("timeHour"); err != nil {
		return metadata, err
	}
	if metadata.TimeDay, err = readIntField("timeDay"); err != nil {
		return metadata, err
	}
	if metadata.TimeMonth, err = readIntField("timeMonth"); err != nil {
		return metadata, err
	}

	yearOffset, err := readIntField("timeYearOffset")
	if err != nil {
		return metadata, err
	}
	metadata.TimeYear = yearStart + yearOffset

	if metadata.TimeIsDST, err = readIntField("timeIsDst"); err != nil {
		return metadata, err
	}
	if metadata.DatabaseName, err = metadata.ReadString(r); err != nil {
		return metadata, err
	}
	if metadata.RemoteVersion, err = metadata.ReadString(r); err != nil {
		return metadata, err
	}
	if metadata.PGDumpVersion, err = metadata.ReadString(r); err != nil {
		return metadata, err
	}
	if metadata.TOCCount, err = readIntField("toccount"); err != nil {
		return metadata, err
	}

	return metadata, nil
}

// ReadExactString reads a string from the reader, numBytes from current position.
func ReadExactString(reader io.Reader, numBytes int) (string, error) {
	buf := make([]byte, numBytes)

	n, err := io.ReadFull(reader, buf)
	if err != nil {
		return "", mapReadErr(err)
	}

	return string(buf[0:n]), nil
}

// ReadExactInt reads an int from the reader, numBytes from current position.
func ReadExactInt(reader io.Reader, numBytes int) (uint8, error) {
	if numBytes != 1 {
		return 0, fmt.Errorf("%w: numBytes=%d", ErrInvalidReadSize, numBytes)
	}

	// Fast path: use ReadByte if available (e.g., bufio.Reader)
	if br, ok := reader.(io.ByteReader); ok {
		b, err := br.ReadByte()
		if err != nil {
			return 0, mapReadErr(err)
		}
		return b, nil
	}

	// Fallback for readers without ReadByte
	var buf [1]byte
	if _, err := io.ReadFull(reader, buf[:]); err != nil {
		return 0, mapReadErr(err)
	}

	return buf[0], nil
}

func mapReadErr(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ErrNeedMoreData
	}

	return err
}
