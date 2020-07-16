package main

import (
	"database/sql"
	"strings"

	"github.com/fnargesian/simhash-lsh"
)

// Metadata of a dataset ID, namely Metadata.ID
// The data is retrieved form the 'Metadata' table in 'opendatalink.sqlite'
type Metadata struct {
	ID           *string // four-by-four (e.g. "ad4f-f5gs")
	Name         *string
	Description  *string
	Attribution  *string
	ContactEmail *string
	UpdatedAt    *string
	Categories   *string // Comma-separated tags
	Tags         *string // Comma-separated tags
	Permalink    *string // Permant link to the dataset
}

// MetadataRows retreives all data in the Metadata table.
func MetadataRows(db *sql.DB) (*[]Metadata, error) {
	rows, err := db.Query("SELECT * from Metadata;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metadataRows []Metadata
	for isNext := rows.Next(); isNext; isNext = rows.Next() {
		var metadata Metadata
		if err := rows.Scan(metadata.ID, metadata.Name,
			metadata.Description, metadata.Attribution, metadata.ContactEmail, 
			metadata.UpdatedAt, metadata.Categories, metadata.Tags,
			metadata.Permalink); err != nil {
			return nil, err
		}
		metadataRows = append(metadataRows, metadata)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &metadataRows, nil
}

// isEmpty sets the string to nil if the semantics of the string indicte 
// that the string is nil.
func isEmpty(attribute *string) bool {
	if *attribute == "" || *attribute == "null" || *attribute == "Null" ||
		*attribute == "NULL" {
		return true
	}
	return false
}

// NameClean returns a cleaned version of Metadata.Name()
// If Metadata.NameClean() == nil, then Metadata.Name() is empty.
func (metadata *Metadata) NameClean() []string {
	if isEmpty(metadata.Name) {
		return nil
	}

	return strings.Fields(*metadata.Name)
}

// DescriptionClean returns a cleaned version of Metadata.Description()
// If Metadata.DescriptionClean() == nil, then Metadata.Description() is empty.
func (metadata *Metadata) DescriptionClean() []string {
	if isEmpty(metadata.Description) {
		return nil
	}

	return strings.Fields(*metadata.Description)
}

// AttributionClean returns a cleaned version of Metadata.Attribution()
// If Metadata.AttributionClean() == nil, then Metadata.Attribution() is empty.
func (metadata *Metadata) AttributionClean() []string {
	if isEmpty(metadata.Attribution) {
		return nil
	}

	return strings.Fields(*metadata.Attribution)
}

// CategoriesClean returns a cleaned version of Metadata.Categories()
// If Metadata.CategoriesClean() == nil, then Metadata.Categories() is empty.
func (metadata *Metadata) CategoriesClean() []string {
	if isEmpty(metadata.Categories) {
		return nil
	}

	return strings.Fields(*metadata.Categories)
}

// TagsClean returns a cleaned version of Metadata.Tags()
// If Metadata.TagsClean() == nil, then Metadata.Tags() is empty.
func (metadata *Metadata) TagsClean() []string {
	if isEmpty(metadata.Tags) {
		return nil
	}

	return strings.Fields(*metadata.Tags)
}

func buildMetadataIndex(db *sql.DB) (Index, error) {
	_, err := MetadataRows(db)
	if err != nil {
		return Index{}, err
	}

	// cleanMetadataRawRows(metadataRawRows)

	return Index{}, nil
}

// Index is a wrapper of simhashlsh.CosineLsh
type Index struct {
	index *simhashlsh.CosineLsh
}

// Query finds the ids of approximate nearest neighbour candidates, in 
// un-sorted order, given the query point.
func (i Index) Query(query []float64) []string {
	return i.index.Query(query)
}