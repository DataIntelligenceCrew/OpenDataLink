#!/bin/sh

# Download Socrata datasets.
# Usage: download_socrata_datasets.sh [app token file]
# Requires curl and jq.
# Datasets are saved to datasets/id/rows.csv and metadata is saved to
# datasets/id/metadata.json where id is the Socrata dataset four-by-four.
# The metadata is an element of the "results" array in the JSON data returned by
# the Socrata Discovery API.

mkdir datasets && cd datasets || exit 1

app_token=$([ -f "$1" ] && head -1 "$1")
discovery_api_url='https://api.us.socrata.com/api/catalog/v1'
scrollid=

while true; do
    res=$(curl --no-progress-meter -f -G "$discovery_api_url" \
        -d only=datasets \
        -d provenance=official \
        -d limit=50 \
        -d scroll_id="$scrollid" \
        -H "X-App-Token: $app_token") || continue

    if [ "$(printf '%s' "$res" | jq '.results | length')" -eq 0 ]; then
        break
    fi
    scrollid=$(printf '%s' "$res" | jq -r '.results[-1].resource.id')

    printf '%s' "$res" | jq -c '.results[]' |
    while IFS= read -r resource; do
        id=$(printf '%s' "$resource" | jq -r '.resource.id')
        echo "downloading dataset $id"
        mkdir "$id"
        printf '%s' "$resource" | jq '.' > "$id/metadata.json"
        domain=$(printf '%s' "$resource" | jq -r '.metadata.domain')
        download_url="$domain/api/views/$id/rows.csv?accessType=DOWNLOAD"
        curl --no-progress-meter -fL -o "$id/rows.csv" "$download_url"
    done
done
