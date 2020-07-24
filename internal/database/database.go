package database

import (
	"database/sql"
	"strings"

	"github.com/ekzhu/lshensemble"
)

type DB struct {
	*sql.DB
}

func New(databasePath string) (*DB, error) {
	db, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

type ColumnSketch struct {
	ColumnID      string
	DatasetID     string
	ColumnName    string
	DistinctCount int
	Minhash       []uint64
}

func (db *DB) ColumnSketch(columnID string) (*ColumnSketch, error) {
	c := ColumnSketch{ColumnID: columnID}
	var minhash []byte

	err := db.QueryRow(`
	SELECT dataset_id, column_name, distinct_count, minhash
	FROM column_sketches
	WHERE column_id = ?`, columnID).Scan(
		&c.DatasetID, &c.ColumnName, &c.DistinctCount, &minhash)
	if err != nil {
		return nil, err
	}

	c.Minhash, err = lshensemble.BytesToSig(minhash)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func (db *DB) DatasetColumns(datasetID string) ([]*ColumnSketch, error) {
	var cols []*ColumnSketch

	rows, err := db.Query(`
	SELECT column_id, column_name, distinct_count, minhash
	FROM column_sketches
	WHERE dataset_id = ?`, datasetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		c := ColumnSketch{DatasetID: datasetID}
		var minhash []byte

		err := rows.Scan(&c.ColumnID, &c.ColumnName, &c.DistinctCount, &minhash)
		if err != nil {
			return nil, err
		}
		c.Minhash, err = lshensemble.BytesToSig(minhash)
		if err != nil {
			return nil, err
		}
		cols = append(cols, &c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

// Metadata is a row retrieved form the 'Metadata' table in
// 'opendatalink.sqlite'
type Metadata struct {
	DatasetID    string // four-by-four (e.g. "ad4f-f5gs")
	Name         string
	Description  string
	Attribution  string
	ContactEmail string
	UpdatedAt    string
	Categories   []string
	Tags         []string
	Permalink    string // Permanent link to the dataset
}

func (db *DB) Metadata(datasetID string) (*Metadata, error) {
	m := Metadata{DatasetID: datasetID}
	var categories, tags string

	err := db.QueryRow(`
	SELECT
		name,
		description,
		attribution,
		contact_email,
		updated_at,
		categories,
		tags,
		permalink
	FROM metadata
	WHERE dataset_id = ?`, datasetID).Scan(
		&m.Name,
		&m.Description,
		&m.Attribution,
		&m.ContactEmail,
		&m.UpdatedAt,
		&categories,
		&tags,
		&m.Permalink)
	if err != nil {
		return nil, err
	}
	if categories != "" {
		m.Categories = SplitCategories(categories)
	}
	if tags != "" {
		m.Tags = SplitTags(tags)
	}

	return &m, nil
}

// SplitCategories splits categories into a []string
func SplitCategories(categories string) []string {
	return strings.Split(categories, ",")
}

// SplitTags splits tags into a []string
func SplitTags(tags string) []string {
	return strings.Split(tags, ",")
}

func (db *DB) DatasetName(datasetID string) (string, error) {
	var name string
	err := db.QueryRow(`
	SELECT name FROM metadata WHERE dataset_id = ?`, datasetID).Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
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
		var categories string
		var tags string
		if err := rows.Scan(&metadata.DatasetID, &metadata.Name,
			&metadata.Description, &metadata.Attribution,
			&metadata.ContactEmail, &metadata.UpdatedAt,
			&categories, &tags, &metadata.Permalink); err != nil {
			return nil, err
		}

		if categories != "" {
			metadata.Categories = SplitCategories(categories)
		}
		if tags != "" {
			metadata.Tags = SplitTags(tags)
		}
		
		metadataRows = append(metadataRows, metadata)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &metadataRows, nil
}

// // NameClean returns a cleaned version of Metadata.Name()
// func (metadata *Metadata) NameClean() []string {
// 	return strings.Fields(*metadata.Name)
// }

// // DescriptionClean returns a cleaned version of Metadata.Description()
// func (metadata *Metadata) DescriptionClean() []string {
// 	return strings.Fields(*metadata.Description)
// }

// // AttributionClean returns a cleaned version of Metadata.Attribution()
// func (metadata *Metadata) AttributionClean() []string {
// 	return strings.Fields(*metadata.Attribution)
// }
