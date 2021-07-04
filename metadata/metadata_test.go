package metadata_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/mble/pgdump-metadata-extractor/metadata"
)

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

func TestReadExactInt(t *testing.T) {
	t.Parallel()

	exp := []byte{0x01, 0x02, 0x03}
	r := bytes.NewReader(exp)
	res := metadata.ReadExactInt(r, 1)

	if res != 1 {
		t.Errorf("expected=%d, got=%d", 1, res)
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
		DatabaseName:  "empty_db",
		RemoteVersion: "10.11",
		PGDumpVersion: "10.11",
		TOCCount:      15,
	}

	file, err := os.Open("../testdata/min.dump")
	if err != nil {
		t.Error(err)
	}
	defer file.Close()

	metadata, err := metadata.NewMetadata(file)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(exp, metadata) {
		t.Errorf("expected=%+v, got=%+v", exp, metadata)
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

	if !errors.Is(err, io.EOF) {
		t.Errorf("expeceted=%v, got=%v", err, io.EOF)
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
		DatabaseName:  "empty_db",
		RemoteVersion: "10.11",
		PGDumpVersion: "10.11",
		TOCCount:      15,
	}

	exp := `{"magic":"PGDMP","vmain":1,"vmin":13,"vrev":0,"intsize":4,"offsize":8,"format":"CUSTOM","compression":-1,"timeSec":33,"timeMin":53,"timeHour":18,"timeDay":3,"timeMonth":6,"timeYear":2021,"timeIsDst":1,"database":"empty_db","remoteVersion":"10.11","pgDumpVersion":"10.11","toccount":15}`

	json, err := meta.ToJSON()
	if err != nil {
		t.Error(err)
	}

	if string(json) != exp {
		t.Errorf("expected=%s, got=%s", exp, json)
	}
}
