package database

import (
	"database/sql"
	"os"
	"strings"

	"github.com/ekzhu/lshensemble"
	_ "github.com/mattn/go-sqlite3" // Provides the driver for our SQLite database
)

// TestPath is the path to the sqlite database when testing the program
var TestPath = os.Getenv("OPEN_DATA_LINK_DB")

// DB is a wrapper of the opendatalink SQLite3 database
type DB struct {
	*sql.DB
}

// New returns a handle to the database give a path to its file
func New(databasePath string) (*DB, error) {
	db, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

// ColumnSketch represents a row in the column_sketches table
type ColumnSketch struct {
	ColumnID      string
	DatasetID     string
	ColumnName    string
	DistinctCount int
	Minhash       []uint64
}

// ColumnSketch returns the ColumnSketch of the given column
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

// DatasetColumns returns rows which describe the columns of dataset_id
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

// DatasetName returns the name of the dataset given its ID
func (db *DB) DatasetName(datasetID string) (string, error) {
	var name string
	err := db.QueryRow(`
	SELECT name FROM metadata WHERE dataset_id = ?`, datasetID).Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
}

// NameSplit returns a split version of Metadata.Name()
func (metadata *Metadata) NameSplit() []string {
	return strings.Fields(metadata.Name)
}

// DescriptionSplit returns a split version of Metadata.Description()
func (metadata *Metadata) DescriptionSplit() []string {
	return strings.Fields(metadata.Description)
}

// AttributionSplit returns a split version of Metadata.Attribution()
func (metadata *Metadata) AttributionSplit() []string {
	return strings.Fields(metadata.Attribution)
}

// CategoriesSplit splits categories into a []string
func CategoriesSplit(categories string) []string {
	return strings.Split(categories, ",")
}

// TagsSplit splits tags into a []string
func TagsSplit(tags string) []string {
	return strings.Split(tags, ",")
}

// Metadata returns a row given the row's primary key, dataset_id
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
		m.Categories = CategoriesSplit(categories)
	}
	if tags != "" {
		m.Tags = TagsSplit(tags)
	}

	return &m, nil
}

// MetadataRows retreives all data in the Metadata table.
func (db *DB) MetadataRows() (*[]Metadata, error) {
	rows, err := db.Query("SELECT * from metadata;")
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
			metadata.Categories = CategoriesSplit(categories)
		}
		if tags != "" {
			metadata.Tags = TagsSplit(tags)
		}

		metadataRows = append(metadataRows, metadata)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &metadataRows, nil
}
