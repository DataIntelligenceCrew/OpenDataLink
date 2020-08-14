package server

import (
	"sort"

	"github.com/ekzhu/lshensemble"
	"opendatalink/internal/database"
)

type unionabilityResult struct {
	datasetID string
	alignment float64
}

func (s *Server) unionableTables(datasetID string) ([]*unionabilityResult, error) {
	query, err := s.db.DatasetColumns(datasetID)
	if err != nil {
		return nil, err
	}
	candidates, err := s.unionCandidates(query)
	if err != nil {
		return nil, err
	}
	results := make([]*unionabilityResult, 0, len(candidates))

	for _, datasetID := range candidates {
		candidate, err := s.db.DatasetColumns(datasetID)
		if err != nil {
			return nil, err
		}
		alignment := unionabilityScore(query, candidate)
		// Insert result while maintaining sorted order.
		i := sort.Search(len(results), func(i int) bool {
			return results[i].alignment <= alignment
		})
		results = append(results, nil)
		copy(results[i+1:], results[i:])
		results[i] = &unionabilityResult{datasetID, alignment}
	}
	return results, nil
}

func (s *Server) unionCandidates(table []*database.ColumnSketch) ([]string, error) {
	datasetID := table[0].DatasetID
	// Maps dataset IDs to number of joinability query results they appear in.
	joinabilityResults := make(map[string]int)
	// Used to avoid counting the same column multiple times if it appears in
	// the results for multiple queries.
	addedCols := make(map[string]bool)

	for _, c := range table {
		done := make(chan struct{})
		results := s.joinabilityIndex.Query(c.Minhash, c.DistinctCount, 0.5, done)

		// Used to avoid counting the same dataset multiple times for one query.
		added := make(map[string]bool)

		for key := range results {
			colID := key.(string)
			resID := colID[:9]
			if resID == datasetID {
				continue
			}
			if !added[resID] && !addedCols[colID] {
				joinabilityResults[resID]++
			}
			added[resID] = true
			addedCols[colID] = true
		}
		close(done)
	}

	var results []string
	for dataset, count := range joinabilityResults {
		if float64(count)/float64(len(table)) >= 0.4 {
			results = append(results, dataset)
		}
	}
	return results, nil
}

// unionabilityScore returns a score between 0 and 1 that represents the
// unionability of the candidate table with the query table.
// Roughly, it is the fraction of candidate columns that are unionable with a
// query column.
func unionabilityScore(query, candidate []*database.ColumnSketch) float64 {
	var small, big []*database.ColumnSketch
	var qsmall bool

	if len(candidate) < len(query) {
		small, big = candidate, query
	} else {
		small, big = query, candidate
		qsmall = true
	}
	var scores []float64
	matched := make(map[*database.ColumnSketch]bool)

	for _, c1 := range small {
		var best *database.ColumnSketch
		var bestCont float64

		for _, c2 := range big {
			if matched[c2] {
				continue
			}
			var q, x *database.ColumnSketch
			if qsmall {
				q, x = c1, c2
			} else {
				q, x = c2, c1
			}
			cont := lshensemble.Containment(
				q.Minhash, x.Minhash, q.DistinctCount, x.DistinctCount)
			if cont > bestCont {
				best, bestCont = c2, cont
			}
		}
		if best != nil {
			matched[best] = true
			scores = append(scores, bestCont)
		}
	}
	sort.Float64s(scores)
	score := float64(1)
	alignment := 0

	for i := len(scores) - 1; i >= 0; i-- {
		if score < 0.5 {
			break
		}
		score *= scores[i]
		alignment++
	}
	return float64(alignment) / float64(len(query))
}
