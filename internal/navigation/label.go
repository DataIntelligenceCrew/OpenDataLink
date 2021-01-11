package navigation

import (
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	indexpkg "github.com/DataIntelligenceCrew/OpenDataLink/internal/index"
	"github.com/ekzhu/go-fasttext"
)

func (O *TableGraph) labelNodes(db *database.DB, ft *fasttext.FastText) error {
	idx, err := indexpkg.BuildCategoryEmbeddingIndex(db, ft)
	if err != nil {
		return err
	}

	for it := O.Nodes(); it.Next(); {
		node := it.Node().(*Node)
		names, _, err := idx.Query(node.vector, 1)
		if err != nil {
			return err
		}
		node.name = names[0]
		println(node.name)
	}
	return nil
}
