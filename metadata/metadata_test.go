package metadata_test

import (
	"bytes"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/mble/pgdump-metadata-extractor/metadata"
)

func strPtr(s string) *string {
	return &s
}

func encodeSigned(sign byte, val uint64, intSize int) []byte {
	out := make([]byte, 1+intSize)
	out[0] = sign
	for i := 0; i < intSize; i++ {
		out[1+i] = byte(val & 0xff)
		val >>= 8
	}
	return out
}

func encodeInt(val int64, intSize int) []byte {
	sign := byte(0)
	if val < 0 {
		sign = 1
		val = -val
	}

	return encodeSigned(sign, uint64(val), intSize)
}

func encodeString(val *string, intSize int) []byte {
	if val == nil {
		return encodeInt(-1, intSize)
	}

	b := []byte(*val)
	out := append(encodeInt(int64(len(b)), intSize), b...)
	return out
}

func TestReadExactString(t *testing.T) {
	t.Parallel()

	exp := []byte{0x43, 0x41, 0x46, 0x45, 0x0a}
	r := bytes.NewReader(exp)

	res, err := metadata.ReadExactString(r, 5)
	if err != nil {
		t.Error(err)
	}

	if res != string(exp) {
		t.Errorf("expected=%s, got=%s", "CAFE", res)
	}
}

func TestReadExactStringNeedMoreData(t *testing.T) {
	t.Parallel()

	exp := []byte{0x43, 0x41, 0x46}
	r := bytes.NewReader(exp)

	_, err := metadata.ReadExactString(r, 5)
	if !errors.Is(err, metadata.ErrNeedMoreData) {
		t.Errorf("expected=%v, got=%v", metadata.ErrNeedMoreData, err)
	}
}

func TestReadExactIntInvalidNumBytes(t *testing.T) {
	t.Parallel()

	r := bytes.NewReader([]byte{0x01})
	_, err := metadata.ReadExactInt(r, 2)
	if !errors.Is(err, metadata.ErrInvalidReadSize) {
		t.Errorf("expected=%v, got=%v", metadata.ErrInvalidReadSize, err)
	}
}

func TestReadExactInt(t *testing.T) {
	t.Parallel()

	exp := []byte{0x01, 0x02, 0x03}
	r := bytes.NewReader(exp)
	res, err := metadata.ReadExactInt(r, 1)
	if err != nil {
		t.Error(err)
	}

	if res != 1 {
		t.Errorf("expected=%d, got=%d", 1, res)
	}
}

func TestReadExactIntNeedMoreData(t *testing.T) {
	t.Parallel()

	exp := []byte{}
	r := bytes.NewReader(exp)

	_, err := metadata.ReadExactInt(r, 1)
	if !errors.Is(err, metadata.ErrNeedMoreData) {
		t.Errorf("expected=%v, got=%v", metadata.ErrNeedMoreData, err)
	}
}

func TestReadStringNull(t *testing.T) {
	t.Parallel()

	meta := metadata.Metadata{IntSize: 4}
	r := bytes.NewReader(encodeInt(-1, 4))

	res, err := meta.ReadString(r)
	if err != nil {
		t.Error(err)
	}
	if res != nil {
		t.Errorf("expected nil, got=%q", *res)
	}
}

func TestReadStringEmpty(t *testing.T) {
	t.Parallel()

	meta := metadata.Metadata{IntSize: 4}
	r := bytes.NewReader(encodeInt(0, 4))

	res, err := meta.ReadString(r)
	if err != nil {
		t.Error(err)
	}
	if res == nil || *res != "" {
		if res == nil {
			t.Errorf("expected empty string pointer, got nil")
		} else {
			t.Errorf("expected empty string, got=%q", *res)
		}
	}
}

func TestNewMetadata(t *testing.T) {
	t.Parallel()

	exp := metadata.Metadata{
		Magic:         "PGDMP",
		VMain:         1,
		VMin:          13,
		VRev:          0,
		IntSize:       4,
		OffSize:       8,
		Format:        "CUSTOM",
		Compression:   -1,
		TimeSec:       33,
		TimeMin:       53,
		TimeHour:      18,
		TimeDay:       3,
		TimeMonth:     6,
		TimeYear:      2021,
		TimeIsDST:     1,
		DatabaseName:  strPtr("empty_db"),
		RemoteVersion: strPtr("10.11"),
		PGDumpVersion: strPtr("10.11"),
		TOCCount:      15,
	}

	file, err := os.Open("../testdata/min.dump")
	if err != nil {
		t.Error(err)
	}
	defer file.Close()

	meta, err := metadata.NewMetadata(file)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(exp, meta) {
		t.Errorf("expected=%+v, got=%+v", exp, meta)
	}
}

func TestNewMetadataIntSize8(t *testing.T) {
	t.Parallel()

	intSize := 8
	db := "empty_db"
	remote := "10.11"
	pgDump := "10.11"

	var buf bytes.Buffer
	buf.WriteString("PGDMP")
	buf.WriteByte(1)             // vmain
	buf.WriteByte(13)            // vmin
	buf.WriteByte(0)             // vrev
	buf.WriteByte(byte(intSize)) // int size
	buf.WriteByte(8)             // off size
	buf.WriteByte(1)             // format CUSTOM
	buf.Write(encodeInt(-1, intSize))
	buf.Write(encodeInt(33, intSize))
	buf.Write(encodeInt(53, intSize))
	buf.Write(encodeInt(18, intSize))
	buf.Write(encodeInt(3, intSize))
	buf.Write(encodeInt(6, intSize))
	buf.Write(encodeInt(2021-1900, intSize))
	buf.Write(encodeInt(1, intSize))
	buf.Write(encodeString(&db, intSize))
	buf.Write(encodeString(&remote, intSize))
	buf.Write(encodeString(&pgDump, intSize))
	buf.Write(encodeInt(15, intSize))

	meta, err := metadata.NewMetadata(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Error(err)
	}

	exp := metadata.Metadata{
		Magic:         "PGDMP",
		VMain:         1,
		VMin:          13,
		VRev:          0,
		IntSize:       uint8(intSize),
		OffSize:       8,
		Format:        "CUSTOM",
		Compression:   -1,
		TimeSec:       33,
		TimeMin:       53,
		TimeHour:      18,
		TimeDay:       3,
		TimeMonth:     6,
		TimeYear:      2021,
		TimeIsDST:     1,
		DatabaseName:  strPtr("empty_db"),
		RemoteVersion: strPtr("10.11"),
		PGDumpVersion: strPtr("10.11"),
		TOCCount:      15,
	}

	if !reflect.DeepEqual(exp, meta) {
		t.Errorf("expected=%+v, got=%+v", exp, meta)
	}
}

func TestNewMetadataInvalidIntSize(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	buf.WriteString("PGDMP")
	buf.WriteByte(1)  // vmain
	buf.WriteByte(13) // vmin
	buf.WriteByte(0)  // vrev
	buf.WriteByte(0)  // invalid int size

	_, err := metadata.NewMetadata(bytes.NewReader(buf.Bytes()))
	if !errors.Is(err, metadata.ErrInvalidIntSize) {
		t.Errorf("expected=%v, got=%v", metadata.ErrInvalidIntSize, err)
	}
}

func TestNewMetadataInvalidOffSize(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	buf.WriteString("PGDMP")
	buf.WriteByte(1)  // vmain
	buf.WriteByte(13) // vmin
	buf.WriteByte(0)  // vrev
	buf.WriteByte(4)  // int size
	buf.WriteByte(0)  // invalid off size

	_, err := metadata.NewMetadata(bytes.NewReader(buf.Bytes()))
	if !errors.Is(err, metadata.ErrInvalidOffSize) {
		t.Errorf("expected=%v, got=%v", metadata.ErrInvalidOffSize, err)
	}
}

func TestNewMetadataIntOverflowPositive(t *testing.T) {
	t.Parallel()

	maxInt := int(^uint(0) >> 1)
	overflow := uint64(maxInt) + 1

	var buf bytes.Buffer
	buf.WriteString("PGDMP")
	buf.WriteByte(1)  // vmain
	buf.WriteByte(13) // vmin
	buf.WriteByte(0)  // vrev
	buf.WriteByte(8)  // int size
	buf.WriteByte(8)  // off size
	buf.WriteByte(1)  // format CUSTOM
	buf.Write(encodeSigned(0, overflow, 8))

	_, err := metadata.NewMetadata(bytes.NewReader(buf.Bytes()))
	if !errors.Is(err, metadata.ErrIntOverflow) {
		t.Errorf("expected=%v, got=%v", metadata.ErrIntOverflow, err)
	}
}

func TestNewMetadataIntOverflowNegative(t *testing.T) {
	t.Parallel()

	maxInt := int(^uint(0) >> 1)
	overflow := uint64(maxInt) + 2

	var buf bytes.Buffer
	buf.WriteString("PGDMP")
	buf.WriteByte(1)  // vmain
	buf.WriteByte(13) // vmin
	buf.WriteByte(0)  // vrev
	buf.WriteByte(8)  // int size
	buf.WriteByte(8)  // off size
	buf.WriteByte(1)  // format CUSTOM
	buf.Write(encodeSigned(1, overflow, 8))

	_, err := metadata.NewMetadata(bytes.NewReader(buf.Bytes()))
	if !errors.Is(err, metadata.ErrIntOverflow) {
		t.Errorf("expected=%v, got=%v", metadata.ErrIntOverflow, err)
	}
}

func TestNewMetadataMagicErr(t *testing.T) {
	t.Parallel()

	file, err := os.Open("../testdata/not_a.dump")
	if err != nil {
		t.Error(err)
	}
	defer file.Close()

	_, err = metadata.NewMetadata(file)

	if !errors.Is(err, metadata.ErrNotADump) {
		t.Errorf("expected=%v, got=%v", metadata.ErrNotADump, err)
	}
}

func TestNewMetadataReadErr(t *testing.T) {
	t.Parallel()

	file, err := os.Open("../testdata/empty.dump")
	if err != nil {
		t.Error(err)
	}
	defer file.Close()

	_, err = metadata.NewMetadata(file)

	if !errors.Is(err, metadata.ErrNeedMoreData) {
		t.Errorf("expected=%v, got=%v", metadata.ErrNeedMoreData, err)
	}
}

func TestNewMetadataFormat115(t *testing.T) {
	t.Parallel()

	// Format 1.15 uses compression as a string
	intSize := 4
	db := "test_db"
	remote := "14.0"
	pgDump := "14.0"
	compressionSpec := "none"

	var buf bytes.Buffer
	buf.WriteString("PGDMP")
	buf.WriteByte(1)             // vmain
	buf.WriteByte(15)            // vmin (1.15)
	buf.WriteByte(0)             // vrev
	buf.WriteByte(byte(intSize)) // int size
	buf.WriteByte(8)             // off size
	buf.WriteByte(1)             // format CUSTOM
	buf.Write(encodeString(&compressionSpec, intSize))
	buf.Write(encodeInt(33, intSize))
	buf.Write(encodeInt(53, intSize))
	buf.Write(encodeInt(18, intSize))
	buf.Write(encodeInt(3, intSize))
	buf.Write(encodeInt(6, intSize))
	buf.Write(encodeInt(2021-1900, intSize))
	buf.Write(encodeInt(1, intSize))
	buf.Write(encodeString(&db, intSize))
	buf.Write(encodeString(&remote, intSize))
	buf.Write(encodeString(&pgDump, intSize))
	buf.Write(encodeInt(15, intSize))

	meta, err := metadata.NewMetadata(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Error(err)
	}

	exp := metadata.Metadata{
		Magic:           "PGDMP",
		VMain:           1,
		VMin:            15,
		VRev:            0,
		IntSize:         uint8(intSize),
		OffSize:         8,
		Format:          "CUSTOM",
		CompressionSpec: strPtr("none"),
		TimeSec:         33,
		TimeMin:         53,
		TimeHour:        18,
		TimeDay:         3,
		TimeMonth:       6,
		TimeYear:        2021,
		TimeIsDST:       1,
		DatabaseName:    strPtr("test_db"),
		RemoteVersion:   strPtr("14.0"),
		PGDumpVersion:   strPtr("14.0"),
		TOCCount:        15,
	}

	if !reflect.DeepEqual(exp, meta) {
		t.Errorf("expected=%+v, got=%+v", exp, meta)
	}
}

func TestNewMetadataFormat116(t *testing.T) {
	t.Parallel()

	// Format 1.16 uses compression as a single byte
	intSize := 4
	db := "test_db"
	remote := "16.0"
	pgDump := "17.0"

	var buf bytes.Buffer
	buf.WriteString("PGDMP")
	buf.WriteByte(1)             // vmain
	buf.WriteByte(16)            // vmin (1.16)
	buf.WriteByte(0)             // vrev
	buf.WriteByte(byte(intSize)) // int size
	buf.WriteByte(8)             // off size
	buf.WriteByte(1)             // format CUSTOM
	buf.WriteByte(1)             // compression algorithm (single byte)
	buf.Write(encodeInt(55, intSize))
	buf.Write(encodeInt(20, intSize))
	buf.Write(encodeInt(23, intSize))
	buf.Write(encodeInt(14, intSize))
	buf.Write(encodeInt(8, intSize))
	buf.Write(encodeInt(2025-1900, intSize))
	buf.Write(encodeInt(1, intSize))
	buf.Write(encodeString(&db, intSize))
	buf.Write(encodeString(&remote, intSize))
	buf.Write(encodeString(&pgDump, intSize))
	buf.Write(encodeInt(10, intSize))

	meta, err := metadata.NewMetadata(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Error(err)
	}

	exp := metadata.Metadata{
		Magic:         "PGDMP",
		VMain:         1,
		VMin:          16,
		VRev:          0,
		IntSize:       uint8(intSize),
		OffSize:       8,
		Format:        "CUSTOM",
		Compression:   1,
		TimeSec:       55,
		TimeMin:       20,
		TimeHour:      23,
		TimeDay:       14,
		TimeMonth:     8,
		TimeYear:      2025,
		TimeIsDST:     1,
		DatabaseName:  strPtr("test_db"),
		RemoteVersion: strPtr("16.0"),
		PGDumpVersion: strPtr("17.0"),
		TOCCount:      10,
	}

	if !reflect.DeepEqual(exp, meta) {
		t.Errorf("expected=%+v, got=%+v", exp, meta)
	}
}

func TestToJSON(t *testing.T) {
	t.Parallel()

	meta := metadata.Metadata{
		Magic:         "PGDMP",
		VMain:         1,
		VMin:          13,
		VRev:          0,
		IntSize:       4,
		OffSize:       8,
		Format:        "CUSTOM",
		Compression:   -1,
		TimeSec:       33,
		TimeMin:       53,
		TimeHour:      18,
		TimeDay:       3,
		TimeMonth:     6,
		TimeYear:      2021,
		TimeIsDST:     1,
		DatabaseName:  strPtr("empty_db"),
		RemoteVersion: strPtr("10.11"),
		PGDumpVersion: strPtr("10.11"),
		TOCCount:      15,
	}

	exp := `{"magic":"PGDMP","format":"CUSTOM","pgDumpVersion":"10.11","remoteVersion":"10.11","database":"empty_db","timeYear":2021,"timeMonth":6,"timeDay":3,"timeHour":18,"timeMin":53,"timeSec":33,"timeIsDst":1,"compression":-1,"toccount":15,"intsize":4,"vrev":0,"vmin":13,"vmain":1,"offsize":8}`

	json, err := meta.ToJSON()
	if err != nil {
		t.Error(err)
	}

	if string(json) != exp {
		t.Errorf("expected=%s, got=%s", exp, json)
	}
}
