// Command server serves the Open Data Link frontend.
package main

import (
	"log"
	"net/http"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/config"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/index"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/navigation"
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

	organization, err := navigation.BuildInitialOrg(db, &navigation.Config{Gamma: 2.5, TerminationThreshold: 1e-15, TerminationWindow: 25, OperationThreshold: 1e-35})
	log.Println("Built Initial Organization")
	s, err := server.New(&server.Config{
		DevMode:              true,
		DB:                   db,
		MetadataIndex:        metadataIndex,
		JoinabilityThreshold: joinabilityThreshold,
		JoinabilityIndex:     joinabilityIndex,
		Organization:         organization,
	})
	if err != nil {
		log.Fatal(err)
	}
	s.Install()

	log.Fatal(http.ListenAndServe(":8080", nil))
}
