package server

func (s *Server) unionableTables(datasetID string) ([]string, error) {
	queryCols, err := s.db.DatasetColumns(datasetID)
	if err != nil {
		return nil, err
	}
	// Maps dataset IDs to number of joinability query results they appear in.
	joinabilityResults := make(map[string]int)
	// Used to avoid counting the same column multiple times if it appears in
	// the results for multiple queries.
	addedCols := make(map[string]bool)

	for _, col := range queryCols {
		done := make(chan struct{})
		results := s.joinabilityIndex.Query(col.Minhash, col.DistinctCount, 0.5, done)

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
		if float64(count)/float64(len(queryCols)) >= 0.4 {
			results = append(results, dataset)
		}
	}
	return results, nil
}
