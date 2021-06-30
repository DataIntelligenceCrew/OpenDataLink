package index

import (
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/wordemb"
	"github.com/DataIntelligenceCrew/go-faiss"
	"github.com/ekzhu/go-fasttext"

	"strings"
)

// CategoryIndex is an index over the category embedding vectors.
type CategoryIndex struct {
	idx *faiss.IndexFlat
	// Maps ID of vector in index to category name.
	idMap []string
}

// BuildCategoryEmbeddingIndex builds a CategoryIndex.
func BuildCategoryEmbeddingIndex(db *database.DB, ft *fasttext.FastText) (*CategoryIndex, error) {
	categories := make(map[string]bool) // Set of all categories

	rows, err := db.Query(`SELECT categories FROM metadata`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var cat string

		if err := rows.Scan(&cat); err != nil {
			return nil, err
		}

		for _, category := range strings.Split(cat, ",") {
			categories[category] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	index, err := faiss.NewIndexFlatIP(300)
	if err != nil {
		return nil, err
	}

	var idMap []string
	var vecs []float32

	for category := range categories {
		vec, err := wordemb.Vector(ft, []string{category})
		if err != nil {
			if err == wordemb.ErrNoEmb {
				continue
			}
			return nil, err
		}

		idMap = append(idMap, category)
		vecs = append(vecs, vec...)
	}

	if err := index.Add(vecs); err != nil {
		return nil, err
	}

	return &CategoryIndex{index, idMap}, nil
}

// Delete frees the memory associated with the index.
func (idx *CategoryIndex) Delete() {
	idx.idx.Delete()
}

// Query queries the index with vec.
//
// Returns the category names of the (up to) k nearest neighbors and the
// corresponding cosine similarity, sorted by similarity.
func (idx *CategoryIndex) Query(vec []float32, k int64) ([]string, []float32, error) {
	dist, ids, err := idx.idx.Search(vec, k)
	if err != nil {
		return nil, nil, err
	}
	categories := make([]string, 0, k)

	for _, id := range ids {
		if id == -1 {
			break
		}
		categories = append(categories, idx.idMap[id])
	}
	return categories, dist[:len(categories)], nil
}
