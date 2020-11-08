package horizontal

import (
	"testing"

	"opendatalink/internal/database"
)

func TestBuildMetadataIndex(t *testing.T) {
	path, err := database.Path()
	if err != nil {
		t.Fatal(err)
	}
	db, err := database.New(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := BuildMetadataIndex(db); err != nil {
		t.Fatal(err)
	}
}
