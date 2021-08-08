// Package server defines the Server type for serving the Open Data Link
// frontend.
package server

import (
	"bytes"
	"database/sql"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/index"
	nav "github.com/DataIntelligenceCrew/OpenDataLink/internal/navigation"
	"github.com/ekzhu/go-fasttext"
	"github.com/ekzhu/lshensemble"
)

// Server serves the Open Data Link frontend.
type Server struct {
	devMode              bool
	db                   *database.DB
	ft                   *fasttext.FastText
	metadataIndex        *index.MetadataIndex
	joinabilityThreshold float64
	joinabilityIndex     *lshensemble.LshEnsemble
	mux                  sync.Mutex // Guards access to templates
	templates            map[string]*template.Template
	organization         *nav.TableGraph
	organizationConfig   *nav.Config
	organizationGraphSVG []byte
}

// Config is used to configure the server.
type Config struct {
	// If DevMode is true, templates will not be cached.
	DevMode              bool
	DB                   *database.DB
	FastText             *fasttext.FastText
	MetadataIndex        *index.MetadataIndex
	JoinabilityThreshold float64
	JoinabilityIndex     *lshensemble.LshEnsemble
	OrganizeConfig       *nav.Config
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
		ft:                   cfg.FastText,
		templates:            templates,
		metadataIndex:        cfg.MetadataIndex,
		joinabilityThreshold: cfg.JoinabilityThreshold,
		joinabilityIndex:     cfg.JoinabilityIndex,
		organizationConfig:   cfg.OrganizeConfig,
	}, nil
}

// NewHandler returns an HTTP handler that handles requests to the server.
func (s *Server) NewHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/dataset/", s.handleDataset)
	mux.HandleFunc("/search", s.handleSearch)
	mux.HandleFunc("/similar-datasets", s.handleSimilarDatasets)
	mux.HandleFunc("/joinable-columns", s.handleJoinableColumns)
	mux.HandleFunc("/unionable-tables", s.handleUnionableTables)
	mux.HandleFunc("/navigation/", s.handleNav)
	mux.HandleFunc("/navigation-graph", s.handleNavGraph)

	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("web/static"))))

	return panicRecoveryHandler(loggingHandler(mux))
}

func (s *Server) handleNav(w http.ResponseWriter, req *http.Request) {
	nodeID, err := strconv.ParseInt(req.URL.Path[len("/navigation/"):], 10, 64)
	if err != nil {
		nodeID = s.organization.GetRootNode().ID()
	}
	s.servePage(w, "nav", &struct {
		PageTitle string
		Node      *nav.ServeableNode
	}{"Navigation", nav.ToServeableNode(s.organization, s.organization.Node(nodeID))})
}

func (s *Server) handleNavGraph(w http.ResponseWriter, req *http.Request) {
	s.servePage(w, "navigation-graph", &struct {
		PageTitle string
		SVG       template.HTML
	}{"Navigation Graph", template.HTML(s.organizationGraphSVG)})
}

func (s *Server) handleIndex(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/" {
		http.NotFound(w, req)
		return
	}
	s.servePage(w, "index", &struct{ PageTitle string }{"Open Data Link"})
}

func (s *Server) handleDataset(w http.ResponseWriter, req *http.Request) {
	datasetID := req.URL.Path[len("/dataset/"):]

	meta, err := s.db.Metadata(datasetID)
	if err != nil {
		if err == sql.ErrNoRows {
			http.NotFound(w, req)
		} else {
			s.serverError(w, err)
		}
		return
	}
	cols, err := s.db.DatasetColumns(datasetID)
	if err != nil {
		s.serverError(w, err)
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

func (s *Server) handleSearch(w http.ResponseWriter, req *http.Request) {
	query := req.FormValue("q")
	s.organization = nil
	results, err := s.keywordSearch(query)
	if err != nil {
		s.serverError(w, err)
		return
	}
	s.servePage(w, "search", &struct {
		PageTitle string
		Query     string
		Results   []*database.Metadata
	}{
		query + " - Open Data Link",
		query,
		results,
	})
}

func (s *Server) handleSimilarDatasets(w http.ResponseWriter, req *http.Request) {
	queryID := req.FormValue("id")

	results, err := s.similarDatasets(queryID)
	if err != nil {
		if err == sql.ErrNoRows {
			http.NotFound(w, req)
		} else {
			s.serverError(w, err)
		}
		return
	}
	datasetName, err := s.db.DatasetName(queryID)
	if err != nil {
		s.serverError(w, err)
		return
	}
	s.servePage(w, "similar-datasets", &struct {
		PageTitle   string
		DatasetID   string
		DatasetName string
		Results     []*database.Metadata
	}{
		"Similar datasets for " + datasetName + " - Open Data Link",
		queryID,
		datasetName,
		results,
	})
}

func (s *Server) handleJoinableColumns(w http.ResponseWriter, req *http.Request) {
	query, err := s.db.ColumnSketch(req.FormValue("id"))
	if err != nil {
		if err == sql.ErrNoRows {
			http.NotFound(w, req)
		} else {
			s.serverError(w, err)
		}
		return
	}
	results, err := s.joinableColumns(query)
	if err != nil {
		s.serverError(w, err)
		return
	}
	datasetName, err := s.db.DatasetName(query.DatasetID)
	if err != nil {
		s.serverError(w, err)
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
			s.serverError(w, err)
		}
		return
	}
	datasetName, err := s.db.DatasetName(queryID)
	if err != nil {
		s.serverError(w, err)
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

func (s *Server) serverError(w http.ResponseWriter, err error) {
	log.Print(err)
	if s.devMode {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		http.Error(w, http.StatusText(http.StatusInternalServerError),
			http.StatusInternalServerError)
	}
}

func (s *Server) servePage(w http.ResponseWriter, page string, data interface{}) {
	if s.devMode {
		s.mux.Lock()
		defer s.mux.Unlock()
		var err error
		if s.templates, err = parseTemplates(); err != nil {
			s.serverError(w, err)
			return
		}
	}
	tmpl := s.templates[page]
	if tmpl == nil {
		s.serverError(w, fmt.Errorf("servePage: no such page: %s", page))
		return
	}
	// Suppress clickjacking warnings
	w.Header().Set("X-Frame-Options", "SAMEORIGIN")

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		s.serverError(w, err)
		return
	}
	if _, err := buf.WriteTo(w); err != nil {
		s.serverError(w, err)
	}
}

func parseTemplates() (map[string]*template.Template, error) {
	pages := []string{
		"index",
		"dataset",
		"search",
		"similar-datasets",
		"joinable-columns",
		"unionable-tables",
		"nav",
		"navigation-graph",
	}
	templates := make(map[string]*template.Template)

	for _, page := range pages {
		t, err := template.New("base.html").Funcs(template.FuncMap{
			"lines": func(text string) []string {
				return strings.Split(text, "\n")
			},
			"shorten": func(text string) string {
				if len(text) <= 200 {
					return text
				}
				return text[:197] + "..."
			},
			"commaseparate": func(words []string) string {
				return strings.Join(words, ", ")
			},
		}).ParseFiles("web/template/base.html", "web/template/"+page+".html")
		if err != nil {
			return nil, err
		}
		templates[page] = t
	}
	return templates, nil
}

func loggingHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		log.Printf("%s %s", req.Method, req.RequestURI)
		next.ServeHTTP(w, req)
	})
}

func panicRecoveryHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				http.Error(w, http.StatusText(http.StatusInternalServerError),
					http.StatusInternalServerError)
				log.Printf("%s\n%s", err, debug.Stack())
			}
		}()
		next.ServeHTTP(w, req)
	})
}
