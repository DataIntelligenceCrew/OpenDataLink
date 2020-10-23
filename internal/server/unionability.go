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

	for datasetID := range candidates {
		candidate, err := s.db.DatasetColumns(datasetID)
		if err != nil {
			return nil, err
		}
		alignment := unionabilityScore(query, candidate)
		// Keep results sorted in descending order by alignment.
		i := sort.Search(len(results), func(i int) bool {
			return results[i].alignment <= alignment
		})
		results = append(results, nil)
		copy(results[i+1:], results[i:])
		results[i] = &unionabilityResult{datasetID, alignment}
	}
	return results, nil
}

func (s *Server) unionCandidates(table []*database.ColumnSketch) (map[string]bool, error) {
	candidates := make(map[string]bool)
	unionJoinabilityCandidates(table, s.joinabilityIndex, candidates)
	return candidates, nil
}

func unionJoinabilityCandidates(
	table []*database.ColumnSketch,
	index *lshensemble.LshEnsemble,
	candidates map[string]bool,
) {
	datasetID := table[0].DatasetID
	// Maps dataset IDs to number of joinability query results they appear in.
	counts := make(map[string]int)
	// Used to avoid counting the same column multiple times if it appears in
	// the results for multiple queries.
	addedCols := make(map[string]bool)

	for _, c := range table {
		done := make(chan struct{})
		results := index.Query(c.Minhash, c.DistinctCount, 0.5, done)

		// Used to avoid counting the same dataset multiple times for one query.
		added := make(map[string]bool)

		for key := range results {
			colID := key.(string)
			resID := colID[:9]
			if resID == datasetID {
				continue
			}
			if !added[resID] && !addedCols[colID] {
				counts[resID]++
			}
			added[resID] = true
			addedCols[colID] = true
		}
		close(done)
	}

	for dataset, count := range counts {
		if float64(count)/float64(len(table)) >= 0.4 {
			candidates[dataset] = true
		}
	}
}

/*
func unionColNameCandidates(
	table []*database.ColumnSketch,
	db *database.DB,
	candidates map[string]bool,
) error {
	datasetID := table[0].DatasetID
	// Maps dataset IDs to number of column names in common with query.
	counts := make(map[string]int)

	for _, c := range table {
		rows, err := db.Query(`
		SELECT dataset_id,
		`)
	}

	return nil
}
*/

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
			if cont := containment(q, x); cont > bestCont {
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

func sampleContainment(db *database.DB) ([]float64, error) {
	rows, err := db.Query(`
	SELECT column_id FROM column_sketches
	ORDER BY RANDOM() LIMIT 1000`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make([]*database.ColumnSketch, 0, 1000)

	for rows.Next() {
		var columnID string
		if err := rows.Scan(&columnID); err != nil {
			return nil, err
		}
		col, err := db.ColumnSketch(columnID)
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	contSample := make([]float64, 0, 1000000)

	for _, q := range columns {
		for _, x := range columns {
			contSample = append(contSample, containment(q, x))
		}
	}
	sort.Float64s(contSample)
	return contSample, nil
}
