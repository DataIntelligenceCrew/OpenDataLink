package main

import (
	"encoding/csv"
	"fmt"
	"github.com/ekzhu/lshensemble"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
)

type domainKey struct {
	datasetID string
	attribute string
}

type domain struct {
	values map[string]bool
	key    domainKey
}

// Returns a channel of domains read from a CSV file.
func domainsFromCSV(f *os.File, datasetID string) chan domain {
	out := make(chan domain)
	r := csv.NewReader(f)

	records, err := r.ReadAll()
	if err != nil {
		panic(err)
	}
	go func() {
		for col := 0; col < r.FieldsPerRecord; col++ {
			values := make(map[string]bool)
			key := domainKey{datasetID, strconv.Itoa(col)}

			for _, record := range records {
				v := record[col]
				values[v] = true
			}
			out <- domain{values, key}
		}
		close(out)
	}()
	return out
}

// Returns a channel of domains read from a dataset directory.
func readDomains(dir string) chan domain {
	out := make(chan domain)

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	go func() {
		for _, file := range files {
			if !file.IsDir() {
				continue
			}
			datasetDir := file.Name()
			csvfile, err := os.Open(filepath.Join(dir, datasetDir, "rows.csv"))
			if err != nil {
				panic(err)
			}
			for domain := range domainsFromCSV(csvfile, datasetDir) {
				out <- domain
			}
			csvfile.Close()
		}
		close(out)
	}()
	return out
}

func minhashDomain(domain domain, seed int64, numHash int) *lshensemble.DomainRecord {
	mh := lshensemble.NewMinhash(seed, numHash)
	for v := range domain.values {
		mh.Push([]byte(v))
	}
	return &lshensemble.DomainRecord{
		Key:       domain.key,
		Size:      len(domain.values),
		Signature: mh.Signature(),
	}
}

func memStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Fprintln(
		os.Stderr,
		"Alloc:", m.Alloc/1024,
		"TotalAlloc:", m.TotalAlloc/1024,
		"Sys:", m.Sys/1024,
		"Mallocs:", m.Mallocs)
}

func main() {
	// Read and minhash indexed and query domains

	// Minhash seed
	seed := int64(42)
	// Number of hash functions
	numHash := 256

	var domainRecords []*lshensemble.DomainRecord

	for domain := range readDomains("datasets") {
		rec := minhashDomain(domain, seed, numHash)
		domainRecords = append(domainRecords, rec)
		fmt.Println("minhashed", rec.Key)
	}

	var queries []*lshensemble.DomainRecord

	for domain := range domainsFromCSV(os.Stdin, "query") {
		rec := minhashDomain(domain, seed, numHash)
		queries = append(queries, rec)
		fmt.Println("minhashed", rec.Key)
	}

	memStats()

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

	memStats()

	// Run a query for each domain in the query dataset

	// Containment threshold
	threshold := 0.5

	for _, query := range queries {
		// Get the keys of the candidate domains (may contain false positives)
		// through a channel with option to cancel early.
		done := make(chan struct{})
		defer close(done)
		results := index.Query(query.Signature, query.Size, threshold, done)

		for key := range results {
			// TODO: Check the containment
			fmt.Println(key, "can be joined with", query.Key)
		}
	}

	memStats()
}
