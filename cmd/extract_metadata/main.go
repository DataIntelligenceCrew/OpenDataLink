// Command extract_metadata extracts metadata from JSON files and stores it in
// the metadata table of the Open Data Link database.
package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

const (
	datasetsDir  = "datasets"
	databasePath = "opendatalink.sqlite"
)

type metadata struct {
	Resource *struct {
		Name         string
		ID           string
		Description  string
		Attribution  string
		ContactEmail string `json:"contact_email"`
		UpdatedAt    string
	}
	Classification *struct {
		Categories     []string
		Tags           []string
		DomainCategory string   `json:"domain_category"`
		DomainTags     []string `json:"domain_tags"`
	}
	Permalink string
}

func (m *metadata) categories() []string {
	categories := make([]string, len(m.Classification.Categories))
	copy(categories, m.Classification.Categories)
	return removeDuplicates(append(categories, m.Classification.DomainCategory))
}

func (m *metadata) tags() []string {
	tags := make([]string, len(m.Classification.Tags))
	copy(tags, m.Classification.Tags)
	return removeDuplicates(append(tags, m.Classification.DomainTags...))
}

func removeDuplicates(s []string) []string {
	seen := make(map[string]bool)
	i := 0
	for _, v := range s {
		lower := strings.ToLower(v)
		if seen[lower] {
			continue
		}
		seen[lower] = true
		s[i] = v
		i++
	}
	return s[:i]
}

func main() {
	db, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	insertStmt, err := db.Prepare(`
	INSERT INTO metadata (
		dataset_id,
		name,
		description,
		attribution,
		contact_email,
		updated_at,
		categories,
		tags,
		permalink
	)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer insertStmt.Close()

	files, err := ioutil.ReadDir(datasetsDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		path := filepath.Join(datasetsDir, f.Name(), "metadata.json")

		file, err := os.Open(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				log.Print(err)
				continue
			}
			log.Fatal(err)
		}
		var m metadata
		if err := json.NewDecoder(file).Decode(&m); err != nil {
			log.Fatal(err)
		}
		file.Close()

		_, err = insertStmt.Exec(
			m.Resource.ID,
			m.Resource.Name,
			m.Resource.Description,
			m.Resource.Attribution,
			m.Resource.ContactEmail,
			m.Resource.UpdatedAt,
			strings.Join(m.categories(), ","),
			strings.Join(m.tags(), ","),
			m.Permalink)
		if err != nil {
			log.Fatal(err)
		}
	}
}
