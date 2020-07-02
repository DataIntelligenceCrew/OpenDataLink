# REU2020

### Download datasets and metadata

    ./socratadl [socrata app token]

### Sketch datasets

Create the `column_sketches` table:

    sqlite3 opendatalink.sqlite < create_sketch_table.sql

Run `sketchcolumns` to sketch (minhash) dataset columns and store them in the
`column_sketches` table.

    go run sketchcolumns/main.go

### Start server

    go run server/main.go
