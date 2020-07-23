package main

import (
	"database/sql"
	"fmt"
	"html/template"
	"log"
	"net/http"

	"github.com/ekzhu/lshensemble"
	_ "github.com/mattn/go-sqlite3"
	"opendatalink/internal/database"
)

const (
	databasePath = "opendatalink.sqlite"
	// Containment threshold for joinability index
	joinabilityThreshold = 0.5
)

type Server struct {
	db        *database.DB
	templates map[string]*template.Template
}

func (s *Server) handleDataset(w http.ResponseWriter, req *http.Request) {
	datasetID := req.URL.Path[len("/dataset/"):]

	meta, err := s.db.Metadata(datasetID)
	if err != nil {
		if err == sql.ErrNoRows {
			http.NotFound(w, req)
		} else {
			serverError(w, err)
		}
		return
	}
	cols, err := s.db.DatasetColumns(datasetID)
	if err != nil {
		serverError(w, err)
		return
	}
	s.servePage(w, "dataset", &struct {
		*database.Metadata
		Columns []*database.ColumnSketch
	}{meta, cols})
}

func (s *Server) joinableColumnsHandler(index *lshensemble.LshEnsemble) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		query, err := s.db.ColumnSketch(req.FormValue("q"))
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

		type searchResult struct {
			DatasetID   string
			DatasetName string
			ColumnID    string
			ColumnName  string
			Containment float64
		}
		var resultData []searchResult

		for key := range results {
			colID := key.(string)
			// Don't include query in results
			if colID == query.ColumnID {
				continue
			}
			result, err := s.db.ColumnSketch(colID)
			if err != nil {
				serverError(w, err)
				return
			}
			containment := lshensemble.Containment(
				query.Minhash, result.Minhash, query.DistinctCount, result.DistinctCount)
			if containment < joinabilityThreshold {
				continue
			}
			datasetName, err := s.db.DatasetName(result.DatasetID)
			if err != nil {
				serverError(w, err)
				return
			}
			resultData = append(resultData, searchResult{
				result.DatasetID,
				datasetName,
				result.ColumnID,
				result.ColumnName,
				containment,
			})
		}
		qDatasetName, err := s.db.DatasetName(query.DatasetID)
		if err != nil {
			serverError(w, err)
			return
		}
		s.servePage(w, "joinable-columns", &struct {
			DatasetID   string
			DatasetName string
			ColumnName  string
			Results     []searchResult
		}{
			query.DatasetID,
			qDatasetName,
			query.ColumnName,
			resultData,
		})
	}
}

func serverError(w http.ResponseWriter, err error) {
	log.Print(err)
	w.WriteHeader(http.StatusInternalServerError)
}

func (s *Server) servePage(w http.ResponseWriter, page string, data interface{}) {
	tmpl := s.templates[page]
	if tmpl == nil {
		serverError(w, fmt.Errorf("servePage: no such page: %s", page))
		return
	}
	// TODO: Write to a temporary buffer
	if err := tmpl.Execute(w, data); err != nil {
		serverError(w, err)
	}
}

func parseTemplates() (map[string]*template.Template, error) {
	pages := []string{
		"dataset",
		"joinable-columns",
	}
	templates := make(map[string]*template.Template)

	for _, page := range pages {
		tmpl := "template/" + page + ".html"
		var err error
		templates[page], err = template.ParseFiles("template/base.html", tmpl)
		if err != nil {
			return nil, err
		}
	}
	return templates, nil
}

func main() {
	db, err := database.New(databasePath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	templates, err := parseTemplates()
	if err != nil {
		log.Fatal(err)
	}
	joinabilityIndex, err := buildJoinabilityIndex(db)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("built joinability index")

	s := Server{db, templates}

	http.HandleFunc("/dataset/", s.handleDataset)
	http.HandleFunc("/joinable-columns", s.joinableColumnsHandler(joinabilityIndex))

	log.Fatal(http.ListenAndServe(":8080", nil))
}
