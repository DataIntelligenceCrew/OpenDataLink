package server

import (
	"sort"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/ekzhu/lshensemble"
)

type joinabilityResult struct {
	*database.ColumnSketch
	containment float64
}

func (s *Server) joinableColumns(query *database.ColumnSketch) ([]*joinabilityResult, error) {
	done := make(chan struct{})
	defer close(done)
	resultKeys := s.joinabilityIndex.Query(
		query.Minhash, query.DistinctCount, s.joinabilityThreshold, done)

	results := make([]*joinabilityResult, 0, len(resultKeys))

	for key := range resultKeys {
		colID := key.(string)
		if colID == query.ColumnID {
			continue
		}
		res, err := s.db.ColumnSketch(colID)
		if err != nil {
			return nil, err
		}
		containment := lshensemble.Containment(
			query.Minhash, res.Minhash, query.DistinctCount, res.DistinctCount)
		if containment < s.joinabilityThreshold {
			continue
		}
		results = append(results, &joinabilityResult{res, containment})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].containment > results[j].containment
	})

	return results, nil
}
