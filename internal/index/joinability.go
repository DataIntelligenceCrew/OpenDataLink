package index

import (
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/ekzhu/lshensemble"
)

const (
	// Number of minhash hash functions
	mhSize = 256
	// Number of LSH Ensemble partitions
	numPart = 8
	// Maximum value for the minhash LSH parameter K
	// (number of hash functions per band).
	maxK = 4
)

// BuildJoinabilityIndex builds an LSH Ensemble index on the dataset columns.
func BuildJoinabilityIndex(db *database.DB) (*lshensemble.LshEnsemble, error) {
	var domainRecords []*lshensemble.DomainRecord

	rows, err := db.Query(`
	SELECT column_id, distinct_count, minhash
	FROM column_sketches
	ORDER BY distinct_count
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var columnID string
		var distinctCount int
		var minhash []byte

		if err = rows.Scan(&columnID, &distinctCount, &minhash); err != nil {
			return nil, err
		}
		sig, err := lshensemble.BytesToSig(minhash)
		if err != nil {
			return nil, err
		}
		domainRecords = append(domainRecords, &lshensemble.DomainRecord{
			Key:       columnID,
			Size:      distinctCount,
			Signature: sig,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	index, err := lshensemble.BootstrapLshEnsembleEquiDepth(
		numPart, mhSize, maxK, len(domainRecords), lshensemble.Recs2Chan(domainRecords))
	if err != nil {
		return nil, err
	}
	return index, nil
}
