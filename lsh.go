package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/ekzhu/lshensemble"
	"os"
	"sort"
)

type DomainKey struct {
	DatasetID string
	Attribute string
}

func main() {
	// Read query domain from stdin

	queryDomain := make(map[string]bool)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		queryDomain[scanner.Text()] = true
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	// Read indexed domains from dataset files

	var domains []map[string]bool
	// Each key corresponds to the domain at the same index
	var keys []DomainKey

	csvfile, err := os.Open("domains.csv")
	if err != nil {
		panic(err)
	}
	r := csv.NewReader(csvfile)

	records, err := r.ReadAll()
	if err != nil {
		panic(err)
	}

	for row, record := range records {
		for col, field := range record {
			if row == 0 {
				keys = append(keys, DomainKey{"domains", field})
				domains = append(domains, make(map[string]bool))
			} else {
				domains[col][field] = true
			}
		}
	}

	// Initialize the domain records to hold the minhash signatures
	domainRecords := make([]*lshensemble.DomainRecord, len(domains))

	// Minhash seed
	seed := int64(42)
	// Number of hash functions
	numHash := 256

	// Create the domain records with the signatures
	for i := range domains {
		mh := lshensemble.NewMinhash(seed, numHash)
		for v := range domains[i] {
			mh.Push([]byte(v))
		}
		domainRecords[i] = &lshensemble.DomainRecord{
			Key:       keys[i],
			Size:      len(domains[i]),
			Signature: mh.Signature(),
		}
	}

	sort.Sort(lshensemble.BySize(domainRecords))

	// Set the number of partitions
	numPart := 8
	// Set the maximum value for the minhash LSH parameter K
	// (number of hash functions per band).
	maxK := 4

	// Create index using equi-depth partitioning
	// You can also use BootstrapLshEnsemblePlusEquiDepth for better accuracy
	index, err := lshensemble.BootstrapLshEnsembleEquiDepth(
		numPart, numHash, maxK, len(domainRecords), lshensemble.Recs2Chan(domainRecords))
	if err != nil {
		panic(err)
	}

	// Query

	queryMh := lshensemble.NewMinhash(seed, numHash)
	for v := range queryDomain {
		queryMh.Push([]byte(v))
	}

	// Containment threshold
	threshold := 0.5

	// Get the keys of the candidate domains (may contain false positives)
	// through a channel with option to cancel early.
	done := make(chan struct{})
	defer close(done) // Important!!
	results := index.Query(queryMh.Signature(), len(queryDomain), threshold, done)
	fmt.Println()

	for key := range results {
		// ...
		// You may want to include a post-processing step here to remove
		// false positive domains using the actual domain values.
		// ...
		// You can call break here to stop processing results.
		fmt.Println(key)
	}

	// TODO:
	// Keyword search with command line arguments.
}
