// Package database provides a wrapper of sql.DB for working with the Open Data
// Link database.
package database

import (
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/vec32"
	"github.com/ekzhu/lshensemble"
)

// DB is a wrapper of the Open Data Link database.
type DB struct {
	*sql.DB
}

// New open the database.
func New(databasePath string) (*DB, error) {
	db, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

// ColumnSketch is a row of the column_sketches table.
type ColumnSketch struct {
	ColumnID      string
	DatasetID     string
	ColumnName    string
	DistinctCount int
	Minhash       []uint64
	Sample        []string
}

// ColumnSketch returns the ColumnSketch for the given column ID.
func (db *DB) ColumnSketch(columnID string) (*ColumnSketch, error) {
	c := ColumnSketch{ColumnID: columnID}
	var minhash, sample []byte

	err := db.QueryRow(`
	SELECT dataset_id, column_name, distinct_count, minhash, sample
	FROM column_sketches
	WHERE column_id = ?`, columnID).Scan(
		&c.DatasetID, &c.ColumnName, &c.DistinctCount, &minhash, &sample)
	if err != nil {
		return nil, err
	}

	if c.Minhash, err = lshensemble.BytesToSig(minhash); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(sample, &c.Sample); err != nil {
		return nil, err
	}
	return &c, nil
}

// DatasetColumns returns the column sketches for the dataset with the given ID.
func (db *DB) DatasetColumns(datasetID string) ([]*ColumnSketch, error) {
	var cols []*ColumnSketch

	rows, err := db.Query(`
	SELECT column_id, column_name, distinct_count, minhash, sample
	FROM column_sketches
	WHERE dataset_id = ?`, datasetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		c := ColumnSketch{DatasetID: datasetID}
		var minhash, sample []byte

		err := rows.Scan(
			&c.ColumnID, &c.ColumnName, &c.DistinctCount, &minhash, &sample)
		if err != nil {
			return nil, err
		}
		if c.Minhash, err = lshensemble.BytesToSig(minhash); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(sample, &c.Sample); err != nil {
			return nil, err
		}
		cols = append(cols, &c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

// Metadata is a row of the metadata table.
type Metadata struct {
	DatasetID    string
	Name         string
	Description  string
	Attribution  string
	ContactEmail string
	UpdatedAt    string
	Categories   []string
	Tags         []string
	Permalink    string
}

// DatasetName returns the name of a dataset given its ID.
func (db *DB) DatasetName(datasetID string) (string, error) {
	var name string
	err := db.QueryRow(`
	SELECT name FROM metadata WHERE dataset_id = ?`, datasetID).Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
}

// Metadata returns the metadata for the dataset with the given ID.
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
		m.Categories = strings.Split(categories, ",")
	}
	if tags != "" {
		m.Tags = strings.Split(tags, ",")
	}

	return &m, nil
}

// MetadataVector returns the metadata embedding vector for a dataset.
func (db *DB) MetadataVector(datasetID string) ([]float32, error) {
	var emb []byte

	err := db.QueryRow(`
	SELECT emb FROM metadata_vectors WHERE dataset_id = ?`, datasetID).Scan(&emb)
	if err != nil {
		return nil, err
	}
	vec, err := vec32.FromBytes(emb)
	if err != nil {
		return nil, err
	}
	return vec, nil
}
