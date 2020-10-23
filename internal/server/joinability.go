package server

import (
	"sort"

	"github.com/ekzhu/lshensemble"
	"opendatalink/internal/database"
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
		cont := containment(query, res)
		if cont < s.joinabilityThreshold {
			continue
		}
		// Keep results sorted in descending order by containment.
		i := sort.Search(len(results), func(i int) bool {
			return results[i].containment <= cont
		})
		results = append(results, nil)
		copy(results[i+1:], results[i:])
		results[i] = &joinabilityResult{res, cont}
	}
	return results, nil
}

func containment(q, x *database.ColumnSketch) float64 {
	return lshensemble.Containment(
		q.Minhash, x.Minhash, q.DistinctCount, x.DistinctCount)
}
