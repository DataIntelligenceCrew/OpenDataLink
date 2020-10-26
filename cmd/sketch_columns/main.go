// Command sketch_columns sketches dataset columns and stores the sketches in
// the Open Data Link database.
package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
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
	// Number of sample data values
	sampleSize = 20
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
				sample:      make([]string, 0, sampleSize),
			})
		}
	} else {
		for i, v := range record {
			s.columnSketches[i].update(v)
		}
	}
}

type columnSketch struct {
	columnName  string
	minhash     *lshensemble.Minhash
	hyperloglog *hyperloglog.Sketch
	sample      []string
}

func (s *columnSketch) update(v string) {
	if v != "" {
		b := []byte(v)
		s.minhash.Push(b)
		s.hyperloglog.Insert(b)
	}

	if len(s.sample) < sampleSize {
		s.sample = append(s.sample, v)
	}
}

func sketchDataset(path, datasetID string) (*tableSketch, error) {
	csvfile, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error sketching %v: %w", datasetID, err)
	}
	defer csvfile.Close()

	sketch := tableSketch{datasetID: datasetID}
	r := csv.NewReader(csvfile)
	r.LazyQuotes = true
	r.ReuseRecord = true

	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error sketching %v: %w", datasetID, err)
		}
		sketch.update(record)
	}
	if sketch.columnSketches == nil {
		return nil, nil
	}
	return &sketch, nil
}

func writeSketch(stmt *sql.Stmt, sketch *tableSketch) error {
	for i, col := range sketch.columnSketches {
		sample, err := json.Marshal(col.sample)
		if err != nil {
			return fmt.Errorf("error writing sketch %v: %v", sketch.datasetID, err)
		}
		_, err = stmt.Exec(
			fmt.Sprint(sketch.datasetID, "-", i),
			sketch.datasetID,
			col.columnName,
			col.hyperloglog.Estimate(),
			lshensemble.SigToBytes(col.minhash.Signature()),
			sample)
		if err != nil {
			return fmt.Errorf("error writing sketch %v: %v", sketch.datasetID, err)
		}
	}
	return nil
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
	(column_id, dataset_id, column_name, distinct_count, minhash, sample)
	VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer insertStmt.Close()

	for range files {
		if sketch := <-out; sketch != nil {
			if err := writeSketch(insertStmt, sketch); err != nil {
				log.Fatal(err)
			}
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
