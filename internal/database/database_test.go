package database

import "testing"

func TestNew(t *testing.T) {
	_, err := New(TestPath)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMetadataRows(t *testing.T) {
	db, err := New(TestPath)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.MetadataRows(); err != nil {
		t.Fatal(err)
	}
}
