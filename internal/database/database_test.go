package database

import "testing"

func TestNew(t *testing.T) {
	path, err := Path()
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMetadataRows(t *testing.T) {
	path, err := Path()
	if err != nil {
		t.Fatal(err)
	}
	db, err := New(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.MetadataRows(); err != nil {
		t.Fatal(err)
	}
}
