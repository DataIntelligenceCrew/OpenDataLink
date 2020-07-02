package main

import (
	"database/sql"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"sort"

	"github.com/ekzhu/lshensemble"
	_ "github.com/mattn/go-sqlite3"
)

const (
	databasePath = "opendatalink.sqlite"
	// Minhash parameters
	mhSeed = 42
	mhSize = 256
	// Containment threshold
	threshold = 0.5
	// Number of LSH Ensemble partitions
	numPart = 8
	// Maximum value for the minhash LSH parameter K
	// (number of hash functions per band).
	maxK = 4
)

func buildIndex(db *sql.DB) *lshensemble.LshEnsemble {
	var domainRecords []*lshensemble.DomainRecord

	rows, err := db.Query(`
	SELECT column_id, distinct_count, minhash
	FROM column_sketches
	`)
	if err != nil {
		log.Fatalln(err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnID string
		var distinctCount int
		var minhash []byte

		if err = rows.Scan(&columnID, &distinctCount, &minhash); err != nil {
			log.Fatalln(err)
		}
		sig, err := lshensemble.BytesToSig(minhash)
		if err != nil {
			log.Fatalln(err)
		}
		domainRecords = append(domainRecords, &lshensemble.DomainRecord{
			Key:       columnID,
			Size:      distinctCount,
			Signature: sig,
		})
	}
	if err := rows.Err(); err != nil {
		log.Fatalln(err)
	}

	sort.Sort(lshensemble.BySize(domainRecords))

	index, err := lshensemble.BootstrapLshEnsembleEquiDepth(
		numPart, mhSize, maxK, len(domainRecords), lshensemble.Recs2Chan(domainRecords))
	if err != nil {
		log.Fatalln(err)
	}
	return index
}

var page = template.Must(template.New("joinable-columns").Parse(`
<html>
<head>
<title>Open Data Link</title>
</head>
<body>
<h1>Open Data Link</h1>
<h3>Showing joinable tables on <i>{{.ColumnName}}</i></h3>
{{range .Results}}<p>{{.}}</p>{{else}}<p>No results</p>{{end}}
</body>
</html>
`))

func main() {
	db, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		log.Fatalln(err)
	}
	// Build LSH Ensemble index
	index := buildIndex(db)
	log.Println("built index")
	db.Close()

	http.HandleFunc("/joinable-columns", func(w http.ResponseWriter, req *http.Request) {
		db, err := sql.Open("sqlite3", databasePath)
		if err != nil {
			log.Fatalln(err)
		}
		defer db.Close()

		query := req.FormValue("q")

		stmt, err := db.Prepare(`
		SELECT column_id, column_name, distinct_count, minhash
		FROM column_sketches
		WHERE column_id = ?
		`)
		if err != nil {
			log.Fatalln(err)
		}
		defer stmt.Close()

		var columnID string
		var columnName string
		var distinctCount int
		var minhash []byte

		err = stmt.QueryRow(query).Scan(
			&columnID, &columnName, &distinctCount, &minhash)
		// TODO: Handle no rows
		if err != nil {
			log.Fatalln(err)
		}

		data := struct {
			ColumnName string
			Results    []string
		}{ColumnName: columnName}

		sig, err := lshensemble.BytesToSig(minhash)
		if err != nil {
			log.Fatalln(err)
		}
		done := make(chan struct{})
		defer close(done)
		results := index.Query(sig, distinctCount, threshold, done)

		for key := range results {
			// TODO: Check the containment to remove false positives
			data.Results = append(data.Results, key.(string))
		}
		page.Execute(w, data)
	})

	fmt.Println(http.ListenAndServe(":8080", nil))
}
