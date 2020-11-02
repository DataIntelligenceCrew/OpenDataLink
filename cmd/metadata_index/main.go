// Command metadata_index is a command-line interface for testing the metadata
// embedding index.
// It prints the names of the 20 most similar datasets to the query dataset.
package main

import (
	"database/sql"
	"fmt"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/config"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/index"
	_ "github.com/mattn/go-sqlite3"
)

func randomDataset(db *database.DB) string {
	var datasetID string
	err := db.QueryRow(`
	SELECT dataset_id
	FROM metadata_vectors
	ORDER BY RANDOM() LIMIT 1`).Scan(&datasetID)
	if err != nil {
		panic(err)
	}
	return datasetID
}

func main() {
	db, err := database.New(config.DatabasePath())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	idx, err := index.BuildMetadataEmbeddingIndex(db)
	if err != nil {
		panic(err)
	}
	defer idx.Delete()

	for {
		var query string
		fmt.Print("query dataset (random if empty): ")
		fmt.Scanln(&query)

		if query == "" {
			query = randomDataset(db)
		}

		vec, err := db.MetadataVector(query)
		if err != nil {
			if err == sql.ErrNoRows {
				fmt.Println("no such dataset:", query)
				continue
			}
			panic(err)
		}

		queryName, err := db.DatasetName(query)
		if err != nil {
			panic(err)
		}
		fmt.Println("query name:", queryName)
		fmt.Println()

		ids, cos, err := idx.Query(vec, 21)
		if err != nil {
			panic(err)
		}

		for i, datasetID := range ids {
			// Skip query dataset
			if i == 0 {
				continue
			}
			name, err := db.DatasetName(datasetID)
			if err != nil {
				panic(err)
			}
			fmt.Printf("%.3f %v\n", cos[i], name)
		}
		fmt.Println()
	}
}
