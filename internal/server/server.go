package server

import (
	"database/sql"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"

	"github.com/ekzhu/lshensemble"
	"opendatalink/internal/database"
	"opendatalink/internal/index/horizontal"
)

// Server can be installed to serve the Open Data Link frontend.
type Server struct {
	devMode              bool
	db                   *database.DB
	templates            map[string]*template.Template
	joinabilityThreshold float64
	joinabilityIndex     *lshensemble.LshEnsemble
	MetadataIndex        horizontal.Index
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
	metadataIndex, err := buildMetadataIndex(cfg.DB)
	if err != nil {
		return nil, err
	}
	return &Server{
		devMode:              cfg.DevMode,
		db:                   cfg.DB,
		templates:            templates,
		joinabilityThreshold: cfg.JoinabilityThreshold,
		joinabilityIndex:     cfg.JoinabilityIndex,
		MetadataIndex:        metadataIndex,
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
	s.servePage(w, "index", nil)
}

func buildMetadataIndex(db *database.DB) (horizontal.Index, error) {
	index, err := horizontal.BuildMetadataIndex(db)
	if err != nil {
		return horizontal.Index{}, err
	}
	log.Print("server: built metadata index")
	return index, nil
}

func formatDescription(description string) string {
	if len(description) >= 200 {
		return description[:197] + "..."
	}
	return description
}

func formatTags(tags []string) string {
	return strings.Join(tags, ", ")
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
	for _, datasetID := range s.MetadataIndex.Search(query) {
		metadata, err := s.db.Metadata(datasetID)
		if err != nil {
			log.Print(datasetID)
			panic(err)
		}
		results = append(results, &searchResult{
			datasetID,
			metadata.Name,
			formatDescription(metadata.Description),
			formatTags(metadata.Tags),
		})
	}

	s.servePage(w, "search", &struct {
		Query   string
		Results []*searchResult
	}{query, results})
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
	done := make(chan struct{})
	defer close(done)
	results := s.joinabilityIndex.Query(
		query.Minhash, query.DistinctCount, s.joinabilityThreshold, done)

	type queryResult struct {
		DatasetID   string
		DatasetName string
		ColumnID    string
		ColumnName  string
		Containment float64
	}
	var resultData []*queryResult

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
		if containment < s.joinabilityThreshold {
			continue
		}
		datasetName, err := s.db.DatasetName(result.DatasetID)
		if err != nil {
			serverError(w, err)
			return
		}
		resultData = append(resultData, &queryResult{
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
		Results     []*queryResult
	}{
		query.DatasetID,
		qDatasetName,
		query.ColumnName,
		resultData,
	})
}

func (s *Server) handleUnionableTables(w http.ResponseWriter, req *http.Request) {
	queryID := req.FormValue("id")

	queryName, err := s.db.DatasetName(queryID)
	if err != nil {
		serverError(w, err)
		return
	}
	results, err := s.unionableTables(queryID)
	if err != nil {
		serverError(w, err)
		return
	}
	type queryResult struct {
		DatasetID   string
		DatasetName string
	}
	var resultData []*queryResult

	for _, datasetID := range results {
		datasetName, err := s.db.DatasetName(datasetID)
		if err != nil {
			serverError(w, err)
			return
		}
		resultData = append(resultData, &queryResult{
			DatasetID:   datasetID,
			DatasetName: datasetName,
		})
	}

	s.servePage(w, "unionable-tables", &struct {
		DatasetID   string
		DatasetName string
		Results     []*queryResult
	}{
		queryID,
		queryName,
		resultData,
	})
}

func serverError(w http.ResponseWriter, err error) {
	log.Print(err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (s *Server) servePage(w http.ResponseWriter, page string, data interface{}) {
	if s.devMode {
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
