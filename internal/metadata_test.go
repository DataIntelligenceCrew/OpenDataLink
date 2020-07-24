package internal

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"testing"
)

func TestBuildMetadataIndex(t *testing.T) {
	openDataLinkDB, err := sql.Open("sqlite3", "../test/opendatalink.sqlite")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := BuildMetadataIndex(openDataLinkDB); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataRows(t *testing.T) {
	openDataLinkDB, err := sql.Open("sqlite3", "../test/opendatalink.sqlite")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := MetadataRows(openDataLinkDB); err != nil {
		t.Fatal(err)
	}
}

func TestAccessSQLite(t *testing.T) {
	openDataLinkDB, err := sql.Open("sqlite3", "../test/opendatalink.sqlite")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := openDataLinkDB.Query("SELECT dataset_id FROM metadata;")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var datasetIDs []string
	for isNext := rows.Next(); isNext; isNext = rows.Next() {
		var datasetID string
		if err := rows.Scan(&datasetID); err != nil {
			t.Fatal(err)
		}
		datasetIDs = append(datasetIDs, datasetID)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
