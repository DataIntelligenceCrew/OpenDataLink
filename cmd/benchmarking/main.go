package main

import (
	"flag"
	"log"
	"strconv"
	"time"
	"fmt"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/config"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/index"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/navigation"
	"github.com/ekzhu/go-fasttext"
	_ "github.com/mattn/go-sqlite3"
)

var orgGamma = flag.String("orggamma", "", "Gamma to use for organization generation")
var orgWindow = flag.String("orgwin", "", "Termination Window size for organization generation")
var orgSize = flag.String("orgsize", "", "Number of Datasets on which to generate the organization")

var gamma float64 = 30
var window int = 701
var orgsize int = 50

func parseFlags() {
	
}

func main() {
	flag.Parse()
	if *orgGamma != "" {
		tmp, err := strconv.Atoi(*orgGamma)
		if err == nil {
			gamma = float64(tmp)
			log.Println(gamma)
		}
	}
	if *orgWindow != "" {
		tmp, err := strconv.Atoi(*orgWindow)
		if err == nil {
			window = int(tmp)
			log.Println(window)
		}
	}
	if *orgSize != "" {
		tmp, err := strconv.Atoi(*orgSize)
		if err == nil {
			orgsize = int(tmp)
			// log.Println(orgsize)
		} else {
			log.Println(err)
		}
	}

	parseFlags()
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
	// log.Println("built metadata embedding index")

	orgConf := &navigation.Config{
		Gamma:                gamma,
		TerminationThreshold: 1e-9,
		TerminationWindow:    window*(orgsize/50),
		MaxIters:             orgsize * 2000,
	}

	// 5 randomly selected datasets, so that we are always using the same base
	dsIds := [5]string{"kwuj-dram", "j46e-fnm6", "gdrb-rdf9", "n498-fge9", "cd2m-5zgk"}
	dsSize := orgsize / 5

	orgIds := dsIds[:]

	for _, dataset := range dsIds {
		vec, _ := db.MetadataVector(dataset)

		ids, _, _ := metadataIndex.Query(vec, int64(dsSize))

		orgIds = append(orgIds, ids...)
	}

	start := time.Now()
	organization, _ := navigation.BuildOrganization(db, ft, orgConf, orgIds)
	_ = organization
	t := time.Now()
	fmt.Printf("Time:%0.9f\n", t.Sub(start).Seconds())
	fmt.Printf("Size:%d\n", organization.Nodes().Len())
}
