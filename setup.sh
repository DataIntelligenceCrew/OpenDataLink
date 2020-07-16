#!/bin/bash


# Cleanup item left from previous time setup.sh has been run
rm fast_text.sqlite

# Download and build a fast and persistent fastText database
curl https://dl.fbaipublicfiles.com/fasttext/vectors-english/wiki-news-300d-1M-subword.vec.zip --output zip_file.zip
unzip zip_file.zip 
rm zip_file.zip
go build cmd/build_fast_text/main.go
./build_fast_text wiki-news-300d-1M-subword.vec
rm wiki-news-300d-1M-subword.vec


# Download all of the datasets provided by socrata
./download_socrata_datasets.sh


# Sketch the columns of each dataset
go build cmd/sketch_columns/main.go
sqlite3 opendatalink.sqlite < sql/create_column_sketches_table.sql
./sketch_columns


# Add the metadata to opendatalink.sqltie
sqlite3 opendatalink.sqlite < sql/create_metadata_table.sql
./insert_metadata.sh


# Compile the server and produce the binary 'server'
go build cmd/server/main.go