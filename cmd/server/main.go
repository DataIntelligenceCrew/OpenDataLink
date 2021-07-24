// Command server serves the Open Data Link frontend.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/config"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/index"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/navigation"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/server"
	"github.com/ekzhu/go-fasttext"
	_ "github.com/mattn/go-sqlite3"
)

var orgGamma = flag.String("orggamma", "", "Gamma to use for organization generation")
var orgWindow = flag.String("orgwin", "", "Termination Window size for organization generation")

const DEFAULT_GAMMA float64 = 1
const DEFAULT_WINDOW int = 1001

const (
	// Containment threshold for joinability index
	joinabilityThreshold = 0.5
)

func main() {
	flag.Parse()
	var gamma = DEFAULT_GAMMA
	if *orgGamma != "" {
		tmp, err := strconv.Atoi(*orgGamma)
		if err == nil {
			gamma = float64(tmp)
			log.Println(gamma)
		}
	}
	var window = DEFAULT_WINDOW
	if *orgWindow != "" {
		tmp, err := strconv.Atoi(*orgWindow)
		if err == nil {
			window = int(tmp)
			log.Println(window)
		}
	}
	db, err := database.New(config.DatabasePath())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ft := fasttext.NewFastText(config.FasttextPath())
	defer ft.Close()

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

	orgConf := &navigation.Config{
		Gamma:                gamma,
		TerminationThreshold: 1e-9,
		TerminationWindow:    window,
		MaxIters:             1e6,
	}

	releaseMode := os.Getenv("MODE") == "release"

	s, err := server.New(&server.Config{
		DevMode:              !releaseMode,
		DB:                   db,
		FastText:             ft,
		MetadataIndex:        metadataIndex,
		JoinabilityThreshold: joinabilityThreshold,
		JoinabilityIndex:     joinabilityIndex,
		OrganizeConfig:       orgConf,
	})
	if err != nil {
		log.Fatal(err)
	}
	s.Install()

	port := os.Getenv("SERVERPORT")
	if port == "" && releaseMode {
		port = "80"
	} else {
		port = "8080"
	}
	log.Println("serving at http://localhost:" + port)

	log.Fatal(http.ListenAndServe(":"+port, nil))
}
