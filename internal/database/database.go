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

func (db *DB) DatasetName(datasetID string) (string, error) {
	var name string
	err := db.QueryRow(`
	SELECT name FROM metadata WHERE dataset_id = ?`, datasetID).Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
}
