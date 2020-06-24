# REU2020

### Download datasets

    ./socratadl

### Sketch datasets

Create SQLite database:

    sqlite3 opendatalink.sqlite < create_sketch_table.sql

Sketch datasets:

    go run sketchdomains/main.go
