# Open Data Link

## Introduction

Open Data Link is a search engine for open data. It supports the following
search methods:

- Semantic keyword search over metadata
- Similar dataset search using semantic similarity of metadata
- Joinable table search
- Unionable table search

## System overview

There are three main components:

1. The crawler script downloads datasets and metadata from Socrata.
2. `sketch_columns` and `process_metadata` create data sketches and metadata
   embedding vectors.
3. The server builds indices on the data columns and metadata and serves the
   frontend.

## Development guide

### Run crawler

    scripts/download_socrata_datasets.sh [app token file]

### Sketch dataset columns

Create the `column_sketches` table:

    sqlite3 opendatalink.sqlite < sql/create_column_sketches_table.sql

Run `sketch_columns` to sketch (minhash) dataset columns and store them in the
`column_sketches` table:

    go run cmd/sketch_columns/main.go

### Build fastText database

    curl -O https://dl.fbaipublicfiles.com/fasttext/vectors-english/crawl-300d-2M.vec.zip
    unzip crawl-300d-2M.zip
    go run cmd/build_fasttext/main.go < crawl-300d-2M.vec

This will create the `fasttext.sqlite` database.

### Process metadata

Create the `metadata` and `metadata_vectors` tables:

    sqlite3 opendatalink.sqlite < sql/create_metadata_tables.sql

Run `process_metadata`:

    go run cmd/process_metadata/main.go

This will create metadata embedding vectors for each dataset and save them in
the `metadata_vectors` table. The metadata is saved in the `metadata` table.

### Start server

    go run cmd/server/main.go

### Configuring database paths

The server, `sketch_columns`, and `process_metadata` look for databases named
`opendatalink.sqlite` and `fasttext.sqlite` in the current directory by default.
Alternate paths can be specified in the `OPENDATALINK_DB` and `FASTTEXT_DB`
environment variables.
