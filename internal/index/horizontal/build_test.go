package horizontal

import (
	"testing"

	"opendatalink/internal/database"
)

func TestBuildMetadataIndex(t *testing.T) {
	db, err := database.New(database.TestPath)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := BuildMetadataIndex(db); err != nil {
		t.Fatal(err)
	}
}
