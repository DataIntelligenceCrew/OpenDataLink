CREATE TABLE column_sketches (
    -- dataset_id followed by a dash and the column number.
    column_id TEXT NOT NULL PRIMARY KEY,
    -- The Socrata dataset four-by-four.
    dataset_id TEXT NOT NULL,
    -- The column name.
    column_name TEXT NOT NULL,
    -- An approximate distinct count of the values.
    distinct_count INT NOT NULL,
    -- The minhash signature of the column.
    minhash TEXT NOT NULL
);

CREATE INDEX column_sketches_dataset_idx ON column_sketches(dataset_id);
