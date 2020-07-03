#!/bin/sh

# Extract metadata from JSON files and store it the metadata table of an SQLite
# database.

set -e

datasets_dir=datasets
database=opendatalink.sqlite

esc() {
    sed "s/'/''/g"
}

for d in "$datasets_dir"/*; do
    f="$d/metadata.json"
    [ -f "$f" ] || continue

    dataset_id=$(basename "$d")
    name=$(jq -r '.resource.name' "$f" | esc)
    description=$(jq -r '.resource.description' "$f" | esc)
    attribution=$(jq -r '.resource.attribution' "$f" | esc)
    contact_email=$(jq -r '.resource.contact_email' "$f" | esc)
    updated_at=$(jq -r '.resource.updatedAt' "$f" | esc)
    categories=$(jq -r '.classification | .categories[], .domain_category' "$f" |
        sort -fu |     # Remove duplicates
        paste -sd',' | # Join with commas
        esc)
    tags=$(jq -r '.classification | .tags[], .domain_tags[]' "$f" |
        sort -fu |
        paste -sd',' |
        esc)
    permalink=$(jq -r '.permalink' "$f" | esc)

    sqlite3 "$database" <<EOF
INSERT INTO metadata (
    dataset_id,
    name,
    description,
    attribution,
    contact_email,
    updated_at,
    categories,
    tags,
    permalink
)
VALUES (
    '$dataset_id',
    '$name',
    '$description',
    '$attribution',
    '$contact_email',
    '$updated_at',
    '$categories',
    '$tags',
    '$permalink'
)
EOF
done
