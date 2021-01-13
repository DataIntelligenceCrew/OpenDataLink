package navigation

import (
	"gonum.org/v1/gonum/graph"
)

type IDNamePair struct {
	ID      int64
	Name    string
	Dataset string
}

// ServeableNode a data structure containing node information for the frontend
type ServeableNode struct {
	ID        int64
	NodeName  string
	Dataset   string
	ParentIDs []*IDNamePair
	ChildIDs  []*IDNamePair
}

// ToServeableNode converts a node in the organization into a node that is serveable
func ToServeableNode(O *TableGraph, s graph.Node) *ServeableNode {
	var parentIDs []*IDNamePair
	for it := O.To(s.ID()); it.Next(); {
		parentIDs = append(parentIDs, &IDNamePair{ID: it.Node().ID(), Name: it.Node().(*Node).name})
	}

	var childIDs []*IDNamePair
	for it := O.From(s.ID()); it.Next(); {
		if it.Node().(*Node).dataset == "" {
			childIDs = append(childIDs, &IDNamePair{ID: it.Node().ID(), Name: it.Node().(*Node).name})
		} else {
			childIDs = append(childIDs, &IDNamePair{ID: it.Node().ID(), Name: it.Node().(*Node).name, Dataset: it.Node().(*Node).dataset})
		}
	}
	var name = s.(*Node).name
	var dataset = s.(*Node).dataset

	println(name)

	return &ServeableNode{ID: s.ID(), ParentIDs: parentIDs, ChildIDs: childIDs, NodeName: name, Dataset: dataset}
}

func (O *TableGraph) GetRootNode() graph.Node {
	return O.root
}
