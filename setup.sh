#!/bin/bash


# Download all of the datasets provided by socrata
./download_socrata_datasets.sh


# Add the metadata to opendatalink.sqltie
./insert_metadata.sh


# Download and build a fast and persistent fastText database
go build cmd/build_fast_text/main.go

curl https://dl.fbaipublicfiles.com/fasttext/vectors-english/wiki-news-300d-1M-subword.vec.zip --output zip_file.zip
unzip zip_file.zip 
rm zip_file.zip
./build_fast_text wiki-news-300d-1M-subword.vec
rm wiki-news-300d-1M-subword.vec


# Sketch the columns of each dataset
go build cmd/sketch_columns/main.go
./sketch_columns


# Compile the server and produce the binary 'server'
go build cmd/server/main.go