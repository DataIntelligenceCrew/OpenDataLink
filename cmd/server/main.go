package main

import (
	"log"
	"net/http"

	_ "github.com/mattn/go-sqlite3"
	"opendatalink/internal/database"
	"opendatalink/internal/index"
	"opendatalink/internal/server"
)

const (
	// Containment threshold for joinability index
	joinabilityThreshold = 0.5
)

func main() {
	path, err := database.Path()
	if err != nil {
		panic(err)
	}
	db, err := database.New(path)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	joinabilityIndex, err := index.BuildJoinabilityIndex(db)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("built joinability index")

	s, err := server.New(&server.Config{
		DevMode:              true,
		DB:                   db,
		JoinabilityThreshold: joinabilityThreshold,
		JoinabilityIndex:     joinabilityIndex,
	})
	if err != nil {
		log.Fatal(err)
	}
	s.Install()

	log.Fatal(http.ListenAndServe(":8080", nil))
}
