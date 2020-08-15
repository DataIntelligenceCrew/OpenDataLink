package server

import (
	"sort"

	"github.com/ekzhu/lshensemble"
	"opendatalink/internal/database"
)

type joinabilityResult struct {
	columnID    string
	datasetID   string
	columnName  string
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
		// Keep results sorted in descending order by containment.
		i := sort.Search(len(results), func(i int) bool {
			return results[i].containment <= containment
		})
		results = append(results, nil)
		copy(results[i+1:], results[i:])
		results[i] = &joinabilityResult{
			res.ColumnID,
			res.DatasetID,
			res.ColumnName,
			containment,
		}
	}
	return results, nil
}
