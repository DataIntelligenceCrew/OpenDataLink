package internal

import (
	"database/sql"
	"testing"
)

func TestBuildMetadataIndex(t *testing.T) {
	openDataLinkDB, err := sql.Open("sqlite3", "opendatalink.sqlite")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := BuildMetadataIndex(openDataLinkDB); err != nil {
		t.Fatal(err)
	}
}
