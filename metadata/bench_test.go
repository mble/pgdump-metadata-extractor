package metadata_test

import (
	"bytes"
	"testing"

	"github.com/mble/pgdump-metadata-extractor/metadata"
)

func BenchmarkNewMetadataFormat13(b *testing.B) {
	intSize := 4
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

	data := buf.Bytes()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = metadata.NewMetadata(bytes.NewReader(data))
	}
}

func BenchmarkNewMetadataFormat116(b *testing.B) {
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
	buf.WriteByte(1)             // compression algorithm
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

	data := buf.Bytes()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = metadata.NewMetadata(bytes.NewReader(data))
	}
}

func BenchmarkToJSON(b *testing.B) {
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

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = meta.ToJSON()
	}
}

func BenchmarkReadInt(b *testing.B) {
	meta := metadata.Metadata{IntSize: 4}
	data := encodeInt(12345, 4)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		_, _ = meta.ReadInt(r)
	}
}

func BenchmarkReadString(b *testing.B) {
	meta := metadata.Metadata{IntSize: 4}
	s := "test_database_name"
	data := encodeString(&s, 4)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		_, _ = meta.ReadString(r)
	}
}
