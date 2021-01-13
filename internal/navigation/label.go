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

	usedLabels := make(map[string]bool)

	var labelRec func(*Node) error

	labelRec = func(node *Node) error {
		names, _, err := idx.Query(node.vector, 20)
		if err != nil {
			return err
		}

		var i int
		for i = 0; i < 20; i++ {
			if usedLabels[names[i]] {
				break
			}
		}
		var label string
		if i == 20 {
			label = names[0]
		} else {
			label = names[i]
		}
		usedLabels[label] = true
		println(i, label)

		token := make([]byte, 4)
		rand.Read(token)
		node.name = label + " " + hex.EncodeToString(token)

		for it := O.getChildren(node); it.Next(); {
			child := it.Node().(*Node)
			if child.name == "" {
				if err := labelRec(child); err != nil {
					return err
				}
			}
		}
		return nil
	}

	return labelRec(O.root.(*Node))
}
