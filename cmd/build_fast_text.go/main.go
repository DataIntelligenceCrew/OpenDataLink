package main

import (
	"bufio"
	"errors"
	"log"
	"os"

	"github.com/justinfargnoli/go-fasttext"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	ft := fasttext.New("fast_text_db.sqlite")
	defer func() {
		if err := ft.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	if len(os.Args) != 2 {
		log.Fatal(errors.New("Usage: ./build_fast_text.go <'.vec' file>"))
	}
	file, errFileOpen := os.Open(os.Args[1])
	if errFileOpen != nil {
		log.Fatal(errFileOpen)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	errBuildDB := ft.Build(bufio.NewReader(file))
	if errBuildDB != nil {
		log.Fatal(errBuildDB)
	}
}
