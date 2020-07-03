# REU2020

### Download datasets and metadata

    ./socratadl [app token file]

### Sketch datasets

Create the `column_sketches` table:

    sqlite3 opendatalink.sqlite < sql/create_column_sketches_table.sql

Run `sketchcolumns` to sketch (minhash) dataset columns and store them in the
`column_sketches` table.

    go run sketchcolumns/main.go

### Extract metadata

Create the `metadata` table:

    sqlite3 opendatalink.sqlite < sql/create_metadata_table.sql

Run `extractmeta`.

### Start server

    go run server/main.go
