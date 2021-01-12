package navigation

import (
	"encoding/hex"
	"math/rand"

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
		if node.name == "" {
			token := make([]byte, 4)
			rand.Read(token)
			node.name = names[0] + " " + hex.EncodeToString(token)
		}
		println(node.name)
	}
	return nil
}
