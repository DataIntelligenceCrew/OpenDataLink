// Serves the OpenDataLink frontend with CPU profiling enabled
// Command server serves the Open Data Link frontend.
package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/config"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/index"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/navigation"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/server"
	"github.com/ekzhu/go-fasttext"
	_ "github.com/mattn/go-sqlite3"
)

const (
	// Containment threshold for joinability index
	joinabilityThreshold = 0.5
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
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

	pprof.StopCPUProfile()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("built joinability index")
	orgConf := &navigation.Config{
		Gamma:                30,
		TerminationThreshold: 1e-9,
		TerminationWindow:    1000,
		MaxIters:             1750,
	}

	s, err := server.New(&server.Config{
		DevMode:              true,
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

	log.Println("serving at http://localhost:8080")

	log.Fatal(http.ListenAndServe(":8080", nil))
}
