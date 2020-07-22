package main

import (
	"database/sql"
	"html/template"
	"log"
	"net/http"

	"github.com/ekzhu/lshensemble"
	_ "github.com/mattn/go-sqlite3"
	"opendatalink/database"
)

const (
	databasePath = "opendatalink.sqlite"
	// Containment threshold for joinability index
	joinabilityThreshold = 0.5
)

var templates = template.Must(template.ParseFiles("template/joinable-columns.html"))

func serverError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func renderTemplate(w http.ResponseWriter, tmpl string, data interface{}) {
	err := templates.ExecuteTemplate(w, tmpl, &data)
	if err != nil {
		serverError(w, err)
	}
}

func joinableColumnsHandler(db *database.DB, index *lshensemble.LshEnsemble) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		query, err := db.ColumnSketch(req.FormValue("q"))
		if err != nil {
			if err == sql.ErrNoRows {
				http.NotFound(w, req)
			} else {
				serverError(w, err)
			}
			return
		}
		done := make(chan struct{})
		defer close(done)
		results := index.Query(
			query.Minhash, query.DistinctCount, joinabilityThreshold, done)

		qDatasetName, err := db.DatasetName(query.DatasetID)
		if err != nil {
			serverError(w, err)
			return
		}
		type searchResult struct {
			DatasetName string
			ColumnID    string
			ColumnName  string
			Containment float64
		}
		data := struct {
			DatasetName string
			ColumnName  string
			Results     []searchResult
		}{
			DatasetName: qDatasetName,
			ColumnName:  query.ColumnName,
		}

		for key := range results {
			colID := key.(string)
			// Don't include query in results
			if colID == query.ColumnID {
				continue
			}
			result, err := db.ColumnSketch(colID)
			if err != nil {
				serverError(w, err)
				return
			}
			datasetName, err := db.DatasetName(result.DatasetID)
			if err != nil {
				serverError(w, err)
				return
			}
			containment := lshensemble.Containment(
				query.Minhash, result.Minhash, query.DistinctCount, result.DistinctCount)
			if containment < joinabilityThreshold {
				continue
			}
			data.Results = append(data.Results, searchResult{
				datasetName, result.ColumnID, result.ColumnName, containment,
			})
		}
		renderTemplate(w, "joinable-columns.html", &data)
	}
}

func main() {
	db, err := database.New(databasePath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	joinabilityIndex, err := buildJoinabilityIndex(db)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("built joinability index")

	http.HandleFunc("/joinable-columns", joinableColumnsHandler(db, joinabilityIndex))

	log.Fatal(http.ListenAndServe(":8080", nil))
}
