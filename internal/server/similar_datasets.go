package server

import "github.com/DataIntelligenceCrew/OpenDataLink/internal/database"

// similarDatasets returns the metadata of the 20 datasets most similar to the
// query.
func (s *Server) similarDatasets(datasetID string) ([]*database.Metadata, error) {
	vec, err := s.db.MetadataVector(datasetID)
	if err != nil {
		return nil, err
	}
	ids, _, err := s.metadataIndex.Query(vec, 21)
	if err != nil {
		return nil, err
	}
	var results []*database.Metadata

	for _, id := range ids {
		if id == datasetID {
			continue
		}
		meta, err := s.db.Metadata(id)
		if err != nil {
			return nil, err
		}
		results = append(results, meta)
	}
	return results, nil
}
