#!/bin/bash


go install cmd/build_fast_text/main.go
go install cmd/insert_metadata_embedding_vectors/main.go
go install cmd/insert_metadata_sketches/main.go
go install cmd/server/main.go
go install cmd/sketch_columns/main.go


# todo: download socrata datasets


# todo: insert data into sqlite 


mkdir fast_text
curl https://dl.fbaipublicfiles.com/fasttext/vectors-english/wiki-news-300d-1M-subword.vec.zip --output zip_file.zip
unzip zip_file.zip 
./build_fast_text wiki-news-300d-1M-subword.vec
./insert_metadata_embedding_vectors fast_text_db.sqlite
./insert_metadata_sketches fast_text_db.sqlite
rm -r fast_text
