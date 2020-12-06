package server

import "github.com/DataIntelligenceCrew/OpenDataLink/internal/database"

// keywordSearch performs a keyword search over the dataset metadata.
func (s *Server) keywordSearch(query string) ([]*database.Metadata, error) {
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
