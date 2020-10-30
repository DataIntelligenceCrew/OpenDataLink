package main

import (
	"fmt"

	"gonum.org/v1/gonum/graph/simple"
	"gonum.org/v1/gonum/mat"
)

func main() {
	fmt.Println("Hello, World!")
	tg := newTableGraph(2)
	tg.includeTableNode([]float64{1, 2}, 0)
	tg.includeTableNode([]float64{1, 1}, 1)
	tg.includeTableNode([]float64{2, 2}, 2)
	tg.initialOrg()
}

type tableGraph struct {
	dim int // the dimentionality of node vectors
	*simple.WeightedDirectedGraph
}

func newTableGraph(dim int) tableGraph {
	return tableGraph{
		dim:                   dim,
		WeightedDirectedGraph: simple.NewWeightedDirectedGraph(1, 0),
	}
}

func (g tableGraph) Dim() int { return g.dim }

type node struct {
	vector   *mat.VecDense
	id       int64
	tableIDs []int
	table    bool
}

func (n node) ID() int64             { return n.id }
func (n node) Vector() *mat.VecDense { return n.vector }
func (n node) TableIDs() []int       { return n.tableIDs }

func (g *tableGraph) includeTableNode(vectorSlice []float64, tableID int) {
	vector := mat.NewVecDense(len(vectorSlice), vectorSlice)
	u := g.WeightedDirectedGraph.NewNode()
	uid := u.ID()
	u = node{vector: vector, tableIDs: []int{tableID}, id: uid, table: true}
	g.WeightedDirectedGraph.AddNode(u)
	fmt.Printf("Created node %d with vector %v\n", u.ID(), vectorSlice)
}

func (g *tableGraph) initialOrg() {
	allNodes := g.Nodes()
	m := make(map[int64]node)
	for allNodes.Next() {
		currNode := allNodes.Node()
		m[currNode.ID()] = currNode.(node)
	}
	fmt.Printf("Starting with m length %d\n", len(m))
	for len(m) > 1 {
		var minSim float64 = 1.1
		var minid1, minid2 int64 = 0, 0
		for id1, node1 := range m {
			for id2, node2 := range m {
				if id1 != id2 {
					fmt.Printf("Comparing node %d with vector [%g, %g], and node %d with vector [%g, %g]\n", node1.ID(), node1.Vector().At(0, 0), node1.Vector().At(1, 0), node2.ID(), node2.Vector().At(0, 0), node2.Vector().At(1, 0))
					sim := cosineSim(node1.Vector(), node2.Vector())
					fmt.Printf("sim = %g\n", sim)
					if sim < minSim {
						minSim = sim
						minid1 = id1
						minid2 = id2
					}
				}
			}
		}
		fmt.Printf("Calling to merge node %d and %d\n", minid1, minid2)
		parentNode := g.mergeNodes([]node{m[minid1], m[minid2]}, minSim)
		delete(m, minid1)
		delete(m, minid2)
		m[parentNode.ID()] = parentNode
		fmt.Printf("Now the map is of length %d\n", len(m))
	}
}

func (g *tableGraph) mergeNodes(nodes []node, weight float64) node {
	zeroSlice := make([]float64, g.Dim())
	for i := range zeroSlice {
		zeroSlice[i] = float64(0)
	}
	w := mat.NewVecDense(g.Dim(), zeroSlice)
	newIDs := []int{}
	for _, n := range nodes {
		fmt.Printf("Merging node %d \n", n.ID())
		fmt.Printf("Node %d with vector [%g, %g] \n", n.ID(), n.Vector().At(0, 0), n.Vector().At(1, 0))
		w.AddVec(w, n.Vector())
		newIDs = append(newIDs, n.TableIDs()...)
	}
	dividerSlice := make([]float64, g.Dim())
	for i := range dividerSlice {
		dividerSlice[i] = float64(len(nodes))
	}
	dividerVec := mat.NewVecDense(g.Dim(), dividerSlice)
	w.DivElemVec(w, dividerVec)

	parentNode := g.WeightedDirectedGraph.NewNode()
	uid := parentNode.ID()
	parentNode = node{vector: w, tableIDs: newIDs, id: uid, table: false}
	p := parentNode.(node)
	for _, n := range nodes {
		g.SetWeightedEdge(g.NewWeightedEdge(parentNode, n, weight))
	}
	fmt.Printf("Created parent node %d with vector [%g, %g] \n", p.ID(), p.Vector().At(0, 0), p.Vector().At(1, 0))
	return parentNode.(node)
}

func cosineSim(vector1 *mat.VecDense, vector2 *mat.VecDense) float64 {
	dotProduct := mat.Dot(vector1, vector2)
	magProduct := mat.Norm(vector1, 2) * mat.Norm(vector2, 2)
	// temporarily handle zero vector case
	if magProduct == 0 {
		magProduct = 1
	}
	return dotProduct / magProduct
}
