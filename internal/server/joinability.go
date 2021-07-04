package server

import (
	"sort"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/ekzhu/lshensemble"
)

type joinabilityResult struct {
	*database.ColumnSketch
	DatasetName string
	Containment float64
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
		datasetName, err := s.db.DatasetName(res.DatasetID)
		if err != nil {
			return nil, err
		}
		results = append(results, &joinabilityResult{res, datasetName, containment})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Containment > results[j].Containment
	})
	if len(results) == 0 {
		return results, nil
	}

	orgDatasetIDs := make([]string, 0, 50)
	added := make(map[string]bool)

	for _, res := range results {
		if !added[res.DatasetID] {
			orgDatasetIDs = append(orgDatasetIDs, res.DatasetID)
			added[res.DatasetID] = true
		}
		if len(orgDatasetIDs) == 50 {
			break
		}
	}
	name, err := s.db.DatasetName(query.DatasetID)
	if err != nil {
		return nil, err
	}
	if err := s.buildOrganization(name, orgDatasetIDs); err != nil {
		return nil, err
	}
	return results, nil
}
