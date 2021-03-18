package navigation

import (
	"strings"

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
			if !usedLabels[strings.ToLower(names[i])] {
				break
			}
		}
		var label string
		if i == 20 {
			label = names[0]
		} else {
			label = names[i]
		}

		usedLabels[strings.ToLower(label)] = true

		node.name = label

		for it := O.getChildren(node); it.Next(); {
			child := it.Node().(*Node)
			if child.name == "" {
				if err := labelRec(child); err != nil {
					return err
				}
			} else if O.isLeafNode(child) {
				for jt := O.getParents(child); jt.Next(); {
					jt.Node().(*Node).leafChildren = true
				}
				var name string
				row := db.QueryRow("SELECT name FROM metadata WHERE dataset_id='" + child.name + "';")
				err := row.Scan(&name)
				if err == nil {
					child.name = name
				} else {
					println(err)
				}
			}
		}
		return nil
	}

	return labelRec(O.root.(*Node))
}
