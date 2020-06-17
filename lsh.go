package main

import (
	"encoding/csv"
	"fmt"
	"github.com/axiomhq/hyperloglog"
	"github.com/ekzhu/lshensemble"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
)

type domainKey struct {
	datasetID string
	attribute string
}

type columnSketch struct {
	minhash     *lshensemble.Minhash
	hyperloglog *hyperloglog.Sketch
}

func sketchDataset(f *os.File, datasetID string, mhSeed int64, mhSize int) []*lshensemble.DomainRecord {
	var domainRecords []*lshensemble.DomainRecord
	var columnSketches []*columnSketch

	r := csv.NewReader(f)
	r.ReuseRecord = true

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		if columnSketches == nil {
			columnSketches = make([]*columnSketch, len(record))

			for i := range columnSketches {
				columnSketches[i] = &columnSketch{
					minhash:     lshensemble.NewMinhash(mhSeed, mhSize),
					hyperloglog: hyperloglog.New(),
				}
			}
		}
		for col, sketch := range columnSketches {
			v := []byte(record[col])
			sketch.minhash.Push(v)
			sketch.hyperloglog.Insert(v)
		}
	}
	if columnSketches == nil {
		return nil
	}
	for col, sketch := range columnSketches {
		domainRecords = append(domainRecords, &lshensemble.DomainRecord{
			Key:       domainKey{datasetID, strconv.Itoa(col)},
			Size:      int(sketch.hyperloglog.Estimate()),
			Signature: sketch.minhash.Signature(),
		})
	}
	return domainRecords
}

func main() {
	f, _ := os.Create("cpu.prof")
	defer f.Close()
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	// Read and minhash indexed and query domains

	// Minhash seed
	seed := int64(42)
	// Number of hash functions
	numHash := 256

	var domainRecords []*lshensemble.DomainRecord

	files, err := ioutil.ReadDir("datasets")
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		datasetID := file.Name()
		csvfile, err := os.Open(filepath.Join("datasets", datasetID, "rows.csv"))
		if err != nil {
			panic(err)
		}
		for _, domainRecord := range sketchDataset(csvfile, datasetID, seed, numHash) {
			domainRecords = append(domainRecords, domainRecord)
		}
		csvfile.Close()
		fmt.Println("sketched", datasetID)
	}

	queries := sketchDataset(os.Stdin, "query", seed, numHash)
	fmt.Println("sketched query")

	// Build LSH Ensemble index

	sort.Sort(lshensemble.BySize(domainRecords))

	// Number of partitions
	numPart := 8
	// Maximum value for the minhash LSH parameter K
	// (number of hash functions per band).
	maxK := 4

	// Create index using equi-depth partitioning
	index, err := lshensemble.BootstrapLshEnsembleEquiDepth(
		numPart, numHash, maxK, len(domainRecords), lshensemble.Recs2Chan(domainRecords))
	if err != nil {
		panic(err)
	}

	// Run a query for each domain in the query dataset

	// Containment threshold
	threshold := 0.5

	for _, query := range queries {
		done := make(chan struct{})
		defer close(done)
		results := index.Query(query.Signature, query.Size, threshold, done)

		for key := range results {
			// TODO: Check the containment to remove false positives
			fmt.Println(key, "can be joined with", query.Key)
		}
	}
}
