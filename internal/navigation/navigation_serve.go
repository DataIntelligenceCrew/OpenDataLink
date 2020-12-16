package navigation

import (
	"encoding/json"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/wordparser"
	"gonum.org/v1/gonum/graph"
)

// ServeableNode a data structure containing node information for the frontend
type ServeableNode struct {
	ID        int64
	ParentIDs []int64
	ChildIDs  []int64
}

type NodeName struct {
	ID   int64
	Name string
}

func GetNodeWord(s graph.Node) (*NodeName, error) {
	parser, err := wordparser.New("/localdisk2/opendatalink/fasttext.sqlite")
	if err != nil {
		return nil, err
	}
	out, err := parser.Search(ToDSNode(s).Vector())
	if err != nil {
		return nil, err
	}

	return &NodeName{ID: s.ID(), Name: out}, nil
}

func GetNodeJSON(O *TableGraph, s graph.Node) string {
	var parentIDs []int64
	for it := O.To(s.ID()); it.Next(); {
		parentIDs = append(parentIDs, it.Node().ID())
	}

	var childIDs []int64
	for it := O.From(s.ID()); it.Next(); {
		childIDs = append(childIDs, it.Node().ID())
	}

	out, _ := json.Marshal(ServeableNode{ID: s.ID(), ParentIDs: parentIDs, ChildIDs: childIDs})
	return string(out)
}

// ToServeableNode converts a node in the organization into a node that is serveable
func ToServeableNode(O *TableGraph, s graph.Node) *ServeableNode {
	var parentIDs []int64
	for it := O.To(s.ID()); it.Next(); {
		parentIDs = append(parentIDs, it.Node().ID())
	}

	var childIDs []int64
	for it := O.From(s.ID()); it.Next(); {
		childIDs = append(childIDs, it.Node().ID())
	}

	return &ServeableNode{ID: s.ID(), ParentIDs: parentIDs, ChildIDs: childIDs}
}

func (O *TableGraph) GetRootNode() graph.Node {
	return O.root
}
