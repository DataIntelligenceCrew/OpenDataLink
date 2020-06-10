package main

import (
	"encoding/csv"
	"fmt"
	"github.com/ekzhu/lshensemble"
	"io/ioutil"
	"os"
	"path/filepath"
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

// Reads domains from a CSV file and appends them to a slice.
func domainsFromCSV(domains []domain, f *os.File, datasetID string) []domain {
	r := csv.NewReader(f)

	records, err := r.ReadAll()
	if err != nil {
		panic(err)
	}

	for col := 0; col < r.FieldsPerRecord; col++ {
		values := make(map[string]bool)
		key := domainKey{datasetID, strconv.Itoa(col)}
		fmt.Println("read domain", key)

		for _, record := range records {
			v := record[col]
			values[v] = true
		}
		domains = append(domains, domain{values, key})
	}
	return domains
}

// Reads domains from a dataset directory.
func readDomains(dir string) []domain {
	var domains []domain

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		datasetDir := file.Name()
		csvfile, err := os.Open(filepath.Join(dir, datasetDir, "rows.csv"))
		if err != nil {
			panic(err)
		}
		domains = domainsFromCSV(domains, csvfile, datasetDir)
		csvfile.Close()
	}
	return domains
}

func minhashDomains(domains []domain, seed int64, numHash int) []*lshensemble.DomainRecord {
	domainRecords := make([]*lshensemble.DomainRecord, len(domains))

	for i, domain := range domains {
		mh := lshensemble.NewMinhash(seed, numHash)
		for v := range domain.values {
			mh.Push([]byte(v))
		}
		domainRecords[i] = &lshensemble.DomainRecord{
			Key:       domain.key,
			Size:      len(domain.values),
			Signature: mh.Signature(),
		}
	}
	return domainRecords
}

func main() {
	// Read query dataset from stdin

	var queryDomains []domain
	queryDomains = domainsFromCSV(queryDomains, os.Stdin, "query")

	// Read indexed datasets from files

	domains := readDomains("datasets")

	// Minhash the domains

	// Minhash seed
	seed := int64(42)
	// Number of hash functions
	numHash := 256

	// Create the domain records with the signatures
	domainRecords := minhashDomains(domains, seed, numHash)
	queries := minhashDomains(queryDomains, seed, numHash)

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
}
