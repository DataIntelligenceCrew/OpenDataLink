// Command server serves the Open Data Link frontend.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/config"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/index"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/navigation"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/server"
	"github.com/ekzhu/go-fasttext"
	"github.com/ekzhu/lshensemble"
	_ "github.com/mattn/go-sqlite3"
)

var (
	orgGamma    = flag.Float64("orggamma", 1.0, "Organization gamma parameter")
	orgWindow   = flag.Int("orgwin", 1001, "Organization termination window size")
	noJoinIndex = flag.Bool("nojoin", false, "Disable joinable table search")
)

// Containment threshold for joinability index
const joinabilityThreshold = 0.5

func main() {
	flag.Parse()

	releaseMode := os.Getenv("MODE") == "release"
	if releaseMode {
		log.Println("MODE=release")
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

	var joinabilityIndex *lshensemble.LshEnsemble
	if !*noJoinIndex {
		joinabilityIndex, err = index.BuildJoinabilityIndex(db)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("built joinability index")
	}

	orgConf := &navigation.Config{
		Gamma:                *orgGamma,
		TerminationThreshold: 1e-9,
		TerminationWindow:    *orgWindow,
		MaxIters:             1e6,
	}

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
	if port == "" {
		if releaseMode {
			port = "80"
		} else {
			port = "8080"
		}
	}
	log.Println("serving at http://localhost:" + port)

	log.Fatal(http.ListenAndServe(":"+port, nil))
}
