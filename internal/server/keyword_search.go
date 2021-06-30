package server

import (
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/wordemb"
)

// keywordSearch performs a keyword search over the dataset metadata.
//
// It first tries a semantic search using the metadata embedding index and falls
// back to an exact text search if none of the query words are found in the
// fastText DB.
// For semantic search, the 50 closest matches are returned.
// Text search returns all matches.
func (s *Server) keywordSearch(query string) ([]*database.Metadata, error) {
	vec, err := wordemb.Vector(s.ft, []string{query})
	if err != nil {
		if err == wordemb.ErrNoEmb {
			return s.textSearch(query)
		}
		return nil, err
	}

	ids, _, err := s.metadataIndex.Query(vec, 50)
	if err != nil {
		return nil, err
	}
	var results []*database.Metadata

	for _, id := range ids {
		meta, err := s.db.Metadata(id)
		if err != nil {
			return nil, err
		}
		results = append(results, meta)
	}

	if err := s.buildOrganization(query, ids); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *Server) textSearch(query string) ([]*database.Metadata, error) {
	rows, err := s.db.Query(`
	SELECT dataset_id
	FROM metadata
	WHERE name || description LIKE ?`, "%"+query+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*database.Metadata

	for rows.Next() {
		var datasetID string
		if err := rows.Scan(&datasetID); err != nil {
			return nil, err
		}
		meta, err := s.db.Metadata(datasetID)
		if err != nil {
			return nil, err
		}
		results = append(results, meta)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}
