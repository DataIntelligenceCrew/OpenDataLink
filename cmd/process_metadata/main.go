// Command process_metadata creates metadata embedding vectors and stores the
// metadata and the vectors in the Open Data Link database.
package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/ekzhu/go-fasttext"
	_ "github.com/mattn/go-sqlite3"
	"opendatalink/internal/config"
	"opendatalink/internal/vec32"
)

const datasetsDir = "datasets"

var wordSepRe = regexp.MustCompile(`\W+`)

// Lucene stop words list.
var stopwords = map[string]bool{
	"a":     true,
	"an":    true,
	"and":   true,
	"are":   true,
	"as":    true,
	"at":    true,
	"be":    true,
	"but":   true,
	"by":    true,
	"for":   true,
	"if":    true,
	"in":    true,
	"into":  true,
	"is":    true,
	"it":    true,
	"no":    true,
	"not":   true,
	"of":    true,
	"on":    true,
	"or":    true,
	"such":  true,
	"that":  true,
	"the":   true,
	"their": true,
	"then":  true,
	"there": true,
	"these": true,
	"they":  true,
	"this":  true,
	"to":    true,
	"was":   true,
	"will":  true,
	"with":  true,
}

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

func metadataVector(ft *fasttext.FastText, m *metadata) ([]float32, error) {
	vec := make([]float32, fasttext.Dim)

	metadataText := []string{
		m.Resource.Name,
		m.Resource.Description,
		m.Resource.Attribution,
		strings.Join(m.Classification.Categories, " "),
		strings.Join(m.Classification.Tags, " "),
		m.Classification.DomainCategory,
		strings.Join(m.Classification.DomainTags, " "),
	}
	for _, words := range metadataText {
		for _, word := range wordSepRe.Split(words, -1) {
			if stopwords[strings.ToLower(word)] {
				continue
			}
			emb, err := ft.GetEmb(word)
			if err != nil {
				if err == fasttext.ErrNoEmbFound {
					continue
				}
				return nil, err
			}
			vec32.Normalize(emb)
			vec32.Add(vec, emb)
		}
	}
	vec32.Scale(vec, 1/float32(len(vec)))
	vec32.Normalize(vec)

	return vec, nil
}

func main() {
	db, err := sql.Open("sqlite3", config.DatabasePath())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ft := fasttext.NewFastText(config.FasttextPath())
	defer ft.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	metadataStmt, err := tx.Prepare(`
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
	defer metadataStmt.Close()

	vectorStmt, err := tx.Prepare(`
	INSERT INTO metadata_vectors (dataset_id, emb) VALUES (?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	defer vectorStmt.Close()

	files, err := ioutil.ReadDir(datasetsDir)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		datasetID := f.Name()
		path := filepath.Join(datasetsDir, datasetID, "metadata.json")

		file, err := os.Open(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				log.Print(err)
				continue
			}
			log.Fatalf("dataset %v: %v", datasetID, err)
		}
		var m metadata
		if err := json.NewDecoder(file).Decode(&m); err != nil {
			log.Fatalf("dataset %v: %v", datasetID, err)
		}
		file.Close()

		_, err = metadataStmt.Exec(
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
			log.Fatalf("dataset %v: %v", datasetID, err)
		}

		emb, err := metadataVector(ft, &m)
		if err != nil {
			log.Fatalf("dataset %v: %v", datasetID, err)
		}
		_, err = vectorStmt.Exec(m.Resource.ID, vec32.Bytes(emb))
		if err != nil {
			log.Fatalf("dataset %v: %v", datasetID, err)
		}
	}
	tx.Commit()
}
