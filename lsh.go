package main

import (
	"fmt"
	"github.com/ekzhu/lshensemble"
	"sort"
	//"encoding/csv"
)

type DomainKey struct {
	DatasetID string
	Attribute string
}

func main() {
	//csvfile, err := os.Open("")

	var domains []map[string]bool
	// Each key corresponds to the domain at the same index
	var keys []DomainKey

	// Obtain domains and keys
	domains = append(domains, map[string]bool{
		"Bath":     true,
		"New York": true,
		"Boston":   true,
	})
	keys = append(keys, DomainKey{"dataset1", "city"})

	domains = append(domains, map[string]bool{
		"Bath":      true,
		"New York":  true,
		"London":    true,
		"Liverpool": true,
		"Boston":    true,
	})
	keys = append(keys, DomainKey{"dataset2", "city"})

	domains = append(domains, map[string]bool{
		"Apple":  true,
		"Orange": true,
		"Pear":   true,
	})
	keys = append(keys, DomainKey{"dataset3", "fruit"})

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
	querySig := domainRecords[0].Signature
	querySize := domainRecords[0].Size

	// Containment threshold
	threshold := 0.5

	// Get the keys of the candidate domains (may contain false positives)
	// through a channel with option to cancel early.
	done := make(chan struct{})
	defer close(done) // Important!!
	results := index.Query(querySig, querySize, threshold, done)

	// It seems query domain is included in results
	for key := range results {
		// ...
		// You may want to include a post-processing step here to remove
		// false positive domains using the actual domain values.
		// ...
		// You can call break here to stop processing results.
		fmt.Println(key)
	}
}
