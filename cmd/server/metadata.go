package main

import (
	"database/sql"
	"github.com/fnargesian/simhash-lsh"
)

type metadataRaw struct {
	ID           *string // four-by-four (e.g. "ad4f-f5gs")
	name         *string
	description  *string
	attribution  *string
	contactEmail *string
	updatedAt    *string
	categories   *string // Comma-separated tags
	tags         *string // Comma-separated tags
	permalink    *string // Permant link to the dataset
}

// metadataRawRows retreives all data in the Metadata table.
func metadataRawRows(db *sql.DB) (*[]metadataRaw, error) {
	rows, err := db.Query("SELECT * from Metadata;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metadataRawRows []metadataRaw
	for isNext := rows.Next(); isNext; isNext = rows.Next() {
		var metadataRawRow metadataRaw
		if err := rows.Scan(metadataRawRow.ID, metadataRawRow.name,
			metadataRawRow.description, metadataRawRow.attribution,
			metadataRawRow.contactEmail, metadataRawRow.updatedAt,
			metadataRawRow.categories, metadataRawRow.tags,
			metadataRawRow.permalink); err != nil {
			return nil, err
		}
		metadataRawRows = append(metadataRawRows, metadataRawRow)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &metadataRawRows, nil
}

// func (metadataRawRow *metadataRaw) clean()  {

// }

// type metadataClean struct {
// 	ID           string
// 	name         []string
// 	description  []string
// 	attribution  []string
// 	contactEmail string
// 	updatedAt    string
// 	categories   []string
// 	tags         []string
// 	permalink    string
// }
// 
// // cleanEmpty sets the string to nil if the semantics of the string indicte 
// // that the string is nil.
// func cleanEmpty(attribute *string) {
// 	if *attribute == "" || *attribute == "null" || *attribute == "Null" ||
// 		*attribute == "NULL" {
// 		attribute = nil
// 	}
// }

// // cleanMetadataRawRows cleans the metadata
// func cleanMetadataRawRows(metadataRawRows *[]metadataRaw) *[]metadataClean {
// 	for _, v := range *metadataRawRows {
		
// 	}
// }


func buildMetadataIndex(db *sql.DB) (Index, error) {
	_, err := metadataRawRows(db)
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