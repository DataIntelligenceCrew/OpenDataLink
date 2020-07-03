CREATE TABLE metadata (
    -- The Socrata dataset four-by-four.
    dataset_id TEXT NOT NULL PRIMARY KEY,
    -- The dataset name.
    name TEXT NOT NULL,
    -- The dataset description.
    description TEXT NOT NULL,
    -- The dataset attribution.
    attribution TEXT NOT NULL,
    -- Contact email.
    contact_email TEXT NOT NULL,
    -- The dataset update timestamp.
    updated_at TEXT NOT NULL,
    -- Comma-separated categories.
    categories TEXT NOT NULL,
    -- Comma-separated tags.
    tags TEXT NOT NULL,
    -- Permanent link of the dataset.
    permalink TEXT NOT NULL
);
