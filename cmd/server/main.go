// Command server serves the Open Data Link frontend.
package main

import (
	"log"
	"net/http"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/config"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/index"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/server"
	_ "github.com/mattn/go-sqlite3"
)

const (
	// Containment threshold for joinability index
	joinabilityThreshold = 0.5
)

func main() {
	db, err := database.New(config.DatabasePath())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	metadataIndex, err := index.BuildMetadataEmbeddingIndex(db)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("built metadata embedding index")

	joinabilityIndex, err := index.BuildJoinabilityIndex(db)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("built joinability index")

	s, err := server.New(&server.Config{
		DevMode:              true,
		DB:                   db,
		MetadataIndex:        metadataIndex,
		JoinabilityThreshold: joinabilityThreshold,
		JoinabilityIndex:     joinabilityIndex,
	})
	if err != nil {
		log.Fatal(err)
	}
	s.Install()

	log.Fatal(http.ListenAndServe(":8080", nil))
}
