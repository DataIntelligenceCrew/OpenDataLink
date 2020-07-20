package main

import (
	"REU2020/index"
	"database/sql"
	"html/template"
	"log"
	"net/http"

	"github.com/ekzhu/lshensemble"
	_ "github.com/mattn/go-sqlite3"
)

const (
	databasePath = "opendatalink.sqlite"
	// Containment threshold for joinability index
	joinabilityThreshold = 0.5
)

var templates = template.Must(template.ParseFiles("templates/joinable-columns.html"))

func serverError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func main() {
	db, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		log.Fatal(err)
	}
	joinabilityIndex, err := index.BuildJoinabilityIndex(db)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("built joinability index")
	db.Close()

	http.HandleFunc("/joinable-columns", func(w http.ResponseWriter, req *http.Request) {
		db, err := sql.Open("sqlite3", databasePath)
		if err != nil {
			serverError(w, err)
			return
		}
		defer db.Close()

		qColID := req.FormValue("q")

		sketchSQL, err := db.Prepare(`
		SELECT dataset_id, column_name, distinct_count, minhash
		FROM column_sketches
		WHERE column_id = ?
		`)
		if err != nil {
			serverError(w, err)
			return
		}
		defer sketchSQL.Close()

		var qDatasetID string
		var qColName string
		var qSize int
		var qMinhash []byte

		err = sketchSQL.QueryRow(qColID).Scan(&qDatasetID, &qColName, &qSize, &qMinhash)
		if err != nil {
			if err == sql.ErrNoRows {
				http.NotFound(w, req)
			} else {
				serverError(w, err)
			}
			return
		}

		nameSQL, err := db.Prepare(`SELECT name FROM metadata WHERE dataset_id = ?`)
		if err != nil {
			serverError(w, err)
			return
		}
		defer nameSQL.Close()

		var qDatasetName string
		if err = nameSQL.QueryRow(qDatasetID).Scan(&qDatasetName); err != nil {
			serverError(w, err)
			return
		}

		type result struct {
			DatasetName string
			ColumnID    string
			ColumnName  string
			Containment float64
		}
		data := struct {
			DatasetName string
			ColumnName  string
			Results     []result
		}{
			DatasetName: qDatasetName,
			ColumnName:  qColName,
		}

		qSig, err := lshensemble.BytesToSig(qMinhash)
		if err != nil {
			serverError(w, err)
			return
		}
		done := make(chan struct{})
		defer close(done)
		results := joinabilityIndex.Query(qSig, qSize, joinabilityThreshold, done)

		for key := range results {
			colID := key.(string)
			// Don't include query in results
			if colID == qColID {
				continue
			}
			var datasetID string
			var colName string
			var size int
			var minhash []byte
			var datasetName string

			err = sketchSQL.QueryRow(colID).Scan(&datasetID, &colName, &size, &minhash)
			if err != nil {
				serverError(w, err)
				return
			}
			err = nameSQL.QueryRow(datasetID).Scan(&datasetName)
			if err != nil {
				serverError(w, err)
				return
			}
			sig, err := lshensemble.BytesToSig(minhash)
			if err != nil {
				serverError(w, err)
				return
			}
			containment := lshensemble.Containment(qSig, sig, qSize, size)
			if containment < joinabilityThreshold {
				continue
			}
			data.Results = append(data.Results, result{datasetName, colID, colName, containment})
		}
		err = templates.ExecuteTemplate(w, "joinable-columns.html", data)
		if err != nil {
			serverError(w, err)
		}
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
