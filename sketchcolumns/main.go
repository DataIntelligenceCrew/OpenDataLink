package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
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
		for i, v := range record {
			s.columnSketches = append(s.columnSketches, &columnSketch{
				columnID:    fmt.Sprint(s.datasetID, "-", i),
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
	columnID    string
	columnName  string
	minhash     *lshensemble.Minhash
	hyperloglog *hyperloglog.Sketch
}

func (s *columnSketch) update(v []byte) {
	s.minhash.Push(v)
	s.hyperloglog.Insert(v)
}

func sketchDataset(datasetID string) {
	csvfile, err := os.Open(filepath.Join(datasetsDir, datasetID, "rows.csv"))
	if os.IsNotExist(err) {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	if err != nil {
		panic(err)
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
			panic(err)
		}
		tableSketch.update(record)
	}
	if tableSketch.columnSketches == nil {
		return
	}

	db, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	for _, sketch := range tableSketch.columnSketches {
		stmt, err := db.Prepare(`
		INSERT INTO column_sketches
		(column_id, dataset_id, column_name, distinct_count, minhash)
		VALUES (?, ?, ?, ?, ?);
		`)
		if err != nil {
			panic(err)
		}
		defer stmt.Close()

		_, err = stmt.Exec(
			sketch.columnID,
			tableSketch.datasetID,
			sketch.columnName,
			sketch.hyperloglog.Estimate(),
			lshensemble.SigToBytes(sketch.minhash.Signature()))
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	f, _ := os.Create("cpu.prof")
	defer f.Close()
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	files, err := ioutil.ReadDir(datasetsDir)
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		datasetID := f.Name()
		fmt.Println("sketching", datasetID)
		sketchDataset(datasetID)
	}
}
