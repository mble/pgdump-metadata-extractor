# pgdump-metadata-extractor

![lint](https://github.com/mble/pgdump-metadata-extractor/actions/workflows/lint.yml/badge.svg)
![test](https://github.com/mble/pgdump-metadata-extractor/actions/workflows/test.yml/badge.svg)

## What is this?

This is a small tool to extract some metadata from `pg_dump` generated dumps of PostgreSQL databases, and present it as JSON.

It does not include the TOC.

## Why is this needed?

Sometimes it's handy to have a structured representation of them metadata of a dump.

## How can I use it?

This works best as a static binary:

```shell
$ make build
$ ./bin/pgdump-metadata-extractor --help
Usage of bin/pgdump-metadata-extractor:
  -filename string
    	dump to read metadata of
  -stdin
    	configure to read from stdin
```

Then you run it with:

```shell
$ ./bin/pgdump-metadata-extractor --filename latest.dump
{"magic":"PGDMP","vmain":1,"vmin":13,"vrev":0,"intsize":4,"offsize":8,"format":"CUSTOM","compression":-1,"timeSec":21,"timeMin":21,"timeHour":17,"timeDay":3,"timeMonth":6,"timeYear":2021,"timeIsDst":1,"database":"bigdb","remoteVersion":"10.11","pgDumpVersion":"10.11","toccount":15}
```

or

```shell
$ ./bin/pgdump-metadata-extractor --stdin < latest.dump
{"magic":"PGDMP","vmain":1,"vmin":13,"vrev":0,"intsize":4,"offsize":8,"format":"CUSTOM","compression":-1,"timeSec":21,"timeMin":21,"timeHour":17,"timeDay":3,"timeMonth":6,"timeYear":2021,"timeIsDst":1,"database":"bigdb","remoteVersion":"10.11","pgDumpVersion":"10.11","toccount":15}
```
