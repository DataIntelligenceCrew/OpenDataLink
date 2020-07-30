package horizontal

import (
	"testing"

	"opendatalink/internal/database"
)

func buildIndex(t *testing.T) Index {
	path, err := database.Path()
	if err != nil {
		t.Fatal(err)
	}
	db, err := database.New(path)
	if err != nil {
		t.Fatal(err)
	}
	index, err := BuildMetadataIndex(db)
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func TestQuery(t *testing.T) {
	index := buildIndex(t)
	index.Search("spread of coronavirus in new york state")
}
