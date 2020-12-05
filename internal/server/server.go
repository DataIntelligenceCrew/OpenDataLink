// Package server defines the Server type, which can be installed to serve the
// Open Data Link frontend.
package server

import (
	"database/sql"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/ekzhu/lshensemble"
)

// Server can be installed to serve the Open Data Link frontend.
type Server struct {
	devMode              bool
	db                   *database.DB
	joinabilityThreshold float64
	joinabilityIndex     *lshensemble.LshEnsemble
	mux                  sync.Mutex // Guards access to templates
	templates            map[string]*template.Template
}

// Config is used to configure the server.
type Config struct {
	// If DevMode is true, templates will not be cached.
	DevMode              bool
	DB                   *database.DB
	JoinabilityThreshold float64
	JoinabilityIndex     *lshensemble.LshEnsemble
}

// New creates a new Server with the given configuration.
func New(cfg *Config) (*Server, error) {
	templates, err := parseTemplates()
	if err != nil {
		return nil, err
	}
	return &Server{
		devMode:              cfg.DevMode,
		db:                   cfg.DB,
		templates:            templates,
		joinabilityThreshold: cfg.JoinabilityThreshold,
		joinabilityIndex:     cfg.JoinabilityIndex,
	}, nil
}

// Install registers the server's HTTP handlers.
func (s *Server) Install() {
	http.HandleFunc("/", s.handleIndex)
	http.HandleFunc("/search", s.handleSearch)
	http.HandleFunc("/dataset/", s.handleDataset)
	http.HandleFunc("/joinable-columns", s.handleJoinableColumns)
	http.HandleFunc("/unionable-tables", s.handleUnionableTables)

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("web/static"))))
}

func (s *Server) handleIndex(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/" {
		http.NotFound(w, req)
		return
	}
	s.servePage(w, "index", &struct{ PageTitle string }{"Open Data Link"})
}

func (s *Server) handleSearch(w http.ResponseWriter, req *http.Request) {
	query := req.FormValue("q")

	type searchResult struct {
		DatasetID   string
		DatasetName string
		Description string
		Tags        string
	}
	var results []*searchResult

	rows, err := s.db.Query(`
	SELECT dataset_id, name, description, tags
	FROM metadata
	WHERE name || description LIKE ?`, "%"+query+"%")
	if err != nil {
		serverError(w, err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var res searchResult
		var description, tags string
		err = rows.Scan(&res.DatasetID, &res.DatasetName, &description, &tags)
		if err != nil {
			serverError(w, err)
			return
		}
		if len(description) <= 200 {
			res.Description = description
		} else {
			res.Description = description[:197] + "..."
		}
		res.Tags = strings.Join(strings.Split(tags, ","), ", ")

		results = append(results, &res)
	}
	if err := rows.Err(); err != nil {
		serverError(w, err)
		return
	}

	s.servePage(w, "search", &struct {
		PageTitle string
		Query     string
		Results   []*searchResult
	}{
		query + " - Open Data Link",
		query,
		results,
	})
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
		PageTitle string
		*database.Metadata
		Columns []*database.ColumnSketch
	}{
		meta.Name + " - Open Data Link",
		meta,
		cols,
	})
}

func (s *Server) handleJoinableColumns(w http.ResponseWriter, req *http.Request) {
	query, err := s.db.ColumnSketch(req.FormValue("id"))
	if err != nil {
		if err == sql.ErrNoRows {
			http.NotFound(w, req)
		} else {
			serverError(w, err)
		}
		return
	}
	results, err := s.joinableColumns(query)
	if err != nil {
		serverError(w, err)
		return
	}
	datasetName, err := s.db.DatasetName(query.DatasetID)
	if err != nil {
		serverError(w, err)
		return
	}
	s.servePage(w, "joinable-columns", &struct {
		PageTitle   string
		DatasetID   string
		DatasetName string
		ColumnName  string
		Results     []*joinabilityResult
	}{
		"Joinable tables for " + datasetName + " - Open Data Link",
		query.DatasetID,
		datasetName,
		query.ColumnName,
		results,
	})
}

func (s *Server) handleUnionableTables(w http.ResponseWriter, req *http.Request) {
	queryID := req.FormValue("id")

	results, err := s.unionableTables(queryID)
	if err != nil {
		if err == errInvalidID {
			http.NotFound(w, req)
		} else {
			serverError(w, err)
		}
		return
	}
	datasetName, err := s.db.DatasetName(queryID)
	if err != nil {
		serverError(w, err)
		return
	}
	s.servePage(w, "unionable-tables", &struct {
		PageTitle   string
		DatasetID   string
		DatasetName string
		Results     []*unionabilityResult
	}{
		"Unionable tables for " + datasetName + " - Open Data Link",
		queryID,
		datasetName,
		results,
	})
}

func serverError(w http.ResponseWriter, err error) {
	log.Print(err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (s *Server) servePage(w http.ResponseWriter, page string, data interface{}) {
	if s.devMode {
		s.mux.Lock()
		defer s.mux.Unlock()
		var err error
		if s.templates, err = parseTemplates(); err != nil {
			serverError(w, err)
			return
		}
	}
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
		"index",
		"search",
		"dataset",
		"joinable-columns",
		"unionable-tables",
	}
	templates := make(map[string]*template.Template)

	for _, page := range pages {
		t, err := template.New("base.html").Funcs(template.FuncMap{
			"lines": func(text string) []string {
				return strings.Split(text, "\n")
			},
		}).ParseFiles("web/template/base.html", "web/template/"+page+".html")
		if err != nil {
			return nil, err
		}
		templates[page] = t
	}
	return templates, nil
}
