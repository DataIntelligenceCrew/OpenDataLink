// Command build_fasttext build a fastText SQLite database.
package main

import (
	"log"
	"os"

	"github.com/ekzhu/go-fasttext"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	ft := fasttext.NewFastText("fasttext.sqlite")
	defer ft.Close()

	if err := ft.BuildDB(os.Stdin); err != nil {
		log.Fatal(err)
	}
}
