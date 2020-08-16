package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"

	"github.com/axiomhq/hyperloglog"
	"github.com/ekzhu/lshensemble"
	_ "github.com/mattn/go-sqlite3"
)

const (
	datasetsDir  = "datasets"
	databasePath = "opendatalink.sqlite"
	// Minhash parameters
	mhSeed = 42
	mhSize = 256
)

type tableSketch struct {
	datasetID      string
	columnSketches []*columnSketch
}

func (s *tableSketch) update(record []string) {
	if s.columnSketches == nil {
		for _, v := range record {
			s.columnSketches = append(s.columnSketches, &columnSketch{
				columnName:  v,
				minhash:     lshensemble.NewMinhash(mhSeed, mhSize),
				hyperloglog: hyperloglog.New(),
			})
		}
	}
	for i, v := range record {
		s.columnSketches[i].update([]byte(v))
	}
}

type columnSketch struct {
	columnName  string
	minhash     *lshensemble.Minhash
	hyperloglog *hyperloglog.Sketch
}

func (s *columnSketch) update(v []byte) {
	s.minhash.Push(v)
	s.hyperloglog.Insert(v)
}

func sketchDataset(path, datasetID string) (*tableSketch, error) {
	csvfile, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer csvfile.Close()

	tableSketch := tableSketch{datasetID: datasetID}
	r := csv.NewReader(csvfile)
	r.LazyQuotes = true
	r.ReuseRecord = true

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		tableSketch.update(record)
	}
	if tableSketch.columnSketches == nil {
		return nil, nil
	}
	return &tableSketch, nil
}

func writeSketch(stmt *sql.Stmt, sketch *tableSketch) {
	for i, colSketch := range sketch.columnSketches {
		_, err := stmt.Exec(
			fmt.Sprint(sketch.datasetID, "-", i),
			sketch.datasetID,
			colSketch.columnName,
			colSketch.hyperloglog.Estimate(),
			lshensemble.SigToBytes(colSketch.minhash.Signature()))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func sketchWorker(jobs <-chan string, out chan<- *tableSketch) {
	for datasetID := range jobs {
		log.Println("sketching", datasetID)
		path := filepath.Join(datasetsDir, datasetID, "rows.csv")
		sketch, err := sketchDataset(path, datasetID)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, csv.ErrFieldCount) {
				log.Println(err)
			} else {
				log.Fatal(err)
			}
		}
		out <- sketch
	}
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	files, err := ioutil.ReadDir(datasetsDir)
	if err != nil {
		log.Fatal(err)
	}
	jobs := make(chan string, len(files))
	out := make(chan *tableSketch, len(files))

	for i := 0; i < 10; i++ {
		go sketchWorker(jobs, out)
	}
	for _, f := range files {
		jobs <- f.Name()
	}
	close(jobs)

	db, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	insertStmt, err := db.Prepare(`
	INSERT INTO column_sketches
	(column_id, dataset_id, column_name, distinct_count, minhash)
	VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer insertStmt.Close()

	for range files {
		if sketch := <-out; sketch != nil {
			writeSketch(insertStmt, sketch)
		}
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal(err)
		}
	}
}
