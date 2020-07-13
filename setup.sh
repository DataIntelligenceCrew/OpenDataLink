#!/bin/bash


go install cmd/build_fast_text/main.go
go install cmd/server/main.go
go install cmd/sketch_columns/main.go


./download_socrata_datasets.sh


./insert_metadata.sh

# Download and build a persistent and fast fastText database
curl https://dl.fbaipublicfiles.com/fasttext/vectors-english/wiki-news-300d-1M-subword.vec.zip --output zip_file.zip
unzip zip_file.zip 
rm zip_file.zip
./build_fast_text wiki-news-300d-1M-subword.vec
rm wiki-news-300d-1M-subword.vec


./server