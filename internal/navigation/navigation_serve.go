package navigation

import "gonum.org/v1/gonum/graph"

// ServeableNode a data structure containing node information for the frontend
type ServeableNode struct {
	id        int64
	parentIDs []int64
	childIDs  []int64
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

	return &ServeableNode{id: s.ID(), parentIDs: parentIDs, childIDs: childIDs}
}

func (O *TableGraph) GetRootNode() graph.Node {
	return O.root
}
