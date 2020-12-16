package navigation

import (
	"container/heap"
	"fmt"
	"io/ioutil"
	"math"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/vec32"
	"github.com/DataIntelligenceCrew/go-faiss"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/encoding/dot"
	"gonum.org/v1/gonum/graph/path"
	"gonum.org/v1/gonum/graph/simple"
)

// Config for an organization
type Config struct {
	gamma     float64
	threshold float64
}

const embeddingDim = 300

// Node a dataset specific node
type Node struct {
	id                 int64
	cachedReachibility float64
	vector             []float32       // Metadata embedding vector for the datasets
	datasets           map[string]bool // Set of dataset IDs of children
}

func (n *Node) Vector() []float32 { return n.vector }

// ID Returns the ID of the node, in order to conform to the graph.Node interface
func (n *Node) ID() int64 { return n.id }

// Datasets returns the set of dataset IDs of the child nodes
func (n *Node) Datasets() map[string]bool { return n.datasets }

func newDatasetNode(id int64, vector []float32, datasetID string) *Node {
	return &Node{
		id:                 id,
		vector:             vector,
		cachedReachibility: 0,
		datasets:           map[string]bool{datasetID: true},
	}
}

func newMergedNode(id int64, a, b *Node) *Node {
	vec := make([]float32, embeddingDim)
	vec32.Add(vec, a.vector)
	vec32.Add(vec, b.vector)
	vec32.Scale(vec, 0.5)
	vec32.Normalize(vec)

	datasets := make(map[string]bool)
	for _, n := range []*Node{a, b} {
		for k, v := range n.datasets {
			datasets[k] = v
		}
	}
	return &Node{id, 0, vec, datasets}
}

// TableGraph the custom graph structure for an organization
type TableGraph struct {
	*simple.DirectedGraph
	config    *Config
	root      graph.Node
	rootPaths path.Shortest
	leafNodes []*Node
}

// NewConfig makes a new organization configuration
func NewConfig(gamma, threshold float64) *Config {
	return newConfig(gamma, threshold)
}

func newConfig(gamma, threshold float64) *Config {
	return &Config{gamma, threshold}
}

func newGraph(cfg *Config) *TableGraph {
	return &TableGraph{simple.NewDirectedGraph(), cfg, nil, path.Shortest{}, make([]*Node, 0)}
}

// addDatasetNodes creates nodes for the datasets and adds them to the graph.
func (O *TableGraph) addDatasetNodes(db *database.DB) error {
	rows, err := db.Query(`
	SELECT dataset_id, emb
	FROM metadata_vectors
	WHERE dataset_id IN (
		SELECT dataset_id
		FROM metadata
		WHERE categories LIKE '%education%'
	)`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var datasetID string
		var emb []byte

		if err := rows.Scan(&datasetID, &emb); err != nil {
			return err
		}
		vec, err := vec32.FromBytes(emb)
		if err != nil {
			return err
		}
		id := O.NewNode().ID()
		var n = newDatasetNode(id, vec, datasetID)
		O.AddNode(n)
		O.leafNodes = append(O.leafNodes, n)
	}
	return rows.Err()
}

func (O *TableGraph) addMergedNode(a, b *Node) *Node {
	id := O.NewNode().ID()
	node := newMergedNode(id, a, b)
	O.AddNode(node)
	O.SetEdge(O.NewEdge(node, a))
	O.SetEdge(O.NewEdge(node, b))
	return node
}

// vectors returns the vectors of the nodes in g and the corresponding IDs.
func (O *TableGraph) vectors() (vectors []float32, ids []int64) {
	for it := O.Nodes(); it.Next(); {
		node := it.Node().(*Node)
		vectors = append(vectors, node.vector...)
		ids = append(ids, node.id)
	}
	return
}

// index is an index over the vectors of nodes in a graph.
type index struct {
	idx *faiss.Index
	g   *TableGraph
}

// nodePair stores the similarity of two nodes.
type nodePair struct {
	a, b   *Node
	cosine float32
}

func buildIndex(g *TableGraph) (*index, error) {
	idx, err := faiss.IndexFactory(embeddingDim, "IDMap,Flat", faiss.MetricInnerProduct)
	if err != nil {
		return nil, err
	}
	if err := idx.AddWithIDs(g.vectors()); err != nil {
		return nil, err
	}
	return &index{idx, g}, nil
}

// ntotal returns the number of indexed vectors.
func (idx *index) ntotal() int64 {
	return idx.idx.Ntotal()
}

func (idx *index) add(n *Node) error {
	return idx.idx.AddWithIDs(n.vector, []int64{n.id})
}

func (idx *index) remove(nodes ...*Node) error {
	var ids []int64
	for _, node := range nodes {
		ids = append(ids, node.id)
	}
	sel, err := faiss.NewIDSelectorBatch(ids)
	if err != nil {
		return err
	}
	defer sel.Delete()

	if _, err := idx.idx.RemoveIDs(sel); err != nil {
		return err
	}
	return nil
}

// query returns a nodePair containing the given node and its nearest neighbor.
//
// The neighbor will not be the query itself if the query is in the index.
// Node: this function may panic if there is only one node in the index.
func (idx *index) query(n *Node) (*nodePair, error) {
	cos, ids, err := idx.idx.Search(n.vector, 2)
	if err != nil {
		return nil, err
	}
	id, sim := ids[0], cos[0]
	if id == n.id {
		id, sim = ids[1], cos[1]
	}
	res := idx.g.Node(id).(*Node)

	return &nodePair{n, res, sim}, nil
}

// allPairs queries the index with all indexed nodes.
func (idx *index) allPairs() ([]*nodePair, error) {
	// Query vectors and IDs
	vecs, qids := idx.g.vectors()

	cos, ids, err := idx.idx.Search(vecs, 2)
	if err != nil {
		return nil, err
	}
	results := make([]*nodePair, len(qids))

	for i, qid := range qids {
		a := idx.g.Node(qid).(*Node)
		id, sim := ids[i*2+1], cos[i*2+1]
		if id == a.id {
			id, sim = ids[i*2], cos[i*2]
		}
		b := idx.g.Node(id).(*Node)

		results[i] = &nodePair{a, b, sim}
	}
	return results, nil
}

func (idx *index) delete() {
	idx.idx.Delete()
}

// priorityQueue is used to get the closest pair of nodes.
// It implements the container/heap interface.
type priorityQueue []*nodePair

func (pq priorityQueue) Len() int           { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool { return pq[i].cosine > pq[j].cosine }
func (pq priorityQueue) Swap(i, j int)      { pq[i], pq[j] = pq[j], pq[i] }

func (pq *priorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*nodePair))
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(*pq)
	item := old[n-1]
	old[n-1] = nil
	*pq = old[:n-1]
	return item
}

// Build Initial Org constructs an organization for initialization purposes
func BuildInitialOrg(db *database.DB, cfg *Config) (*TableGraph, error) {
	return buildInitialOrg(db, cfg)
}

// buildInitialOrg builds the initial organization of the navigation graph.
//
// The initial organization is a binary tree created by joining the most similar
// pairs of nodes under a parent node.
func buildInitialOrg(db *database.DB, cfg *Config) (*TableGraph, error) {
	// Create nodes for all datasets and add them to graph.
	g := newGraph(cfg)
	if err := g.addDatasetNodes(db); err != nil {
		return nil, err
	}
	// Create index over the nodes.
	index, err := buildIndex(g)
	if err != nil {
		return nil, err
	}
	defer index.delete()

	// Query index with all nodes, add (query, NN) pairs to priority queue.
	var pq priorityQueue
	pq, err = index.allPairs()
	if err != nil {
		return nil, err
	}
	heap.Init(&pq)

	// Set of nodes that have been added to the initial organization.
	addedIDs := make(map[int64]bool)

	for {
		// Pop closest pair of nodes off PQ.
		pair := heap.Pop(&pq).(*nodePair)

		// Skip pair if a is already part of the tree.
		if addedIDs[pair.a.id] == true {
			continue
		}
		// Update pair if b is part of the tree.
		if addedIDs[pair.b.id] == true {
			p, err := index.query(pair.a)
			if err != nil {
				return nil, err
			}
			heap.Push(&pq, p)
			continue
		}
		// Remove the pair of nodes from index.
		if err := index.remove(pair.a, pair.b); err != nil {
			return nil, err
		}
		// Create merged node from closest pair and add it to graph with edges
		// to the pair of nodes.
		node := g.addMergedNode(pair.a, pair.b)
		addedIDs[pair.a.id] = true
		addedIDs[pair.b.id] = true

		if index.ntotal() == 0 {
			g.root = node
			break
		}
		// Add the merged node to the PQ and index.
		p, err := index.query(node)
		if err != nil {
			return nil, err
		}
		heap.Push(&pq, p)

		if err := index.add(node); err != nil {
			return nil, err
		}
	}
	g.rootPaths = path.DijkstraFrom(g.root, g.DirectedGraph)
	return g, nil
}

func ToDSNode(s graph.Node) *Node {
	original, ok := s.(*Node)
	if ok {
		return original
	}
	print("Cast Failed")
	return nil
}

// $\kappa$ from the paper. Simply the Cosine Similarity
func similarity(a []float32, b []float32) float32 { // TODO: Is something like this already in FAISS?
	aDotB := vec32.Dot(a, b)
	normAB := vec32.Norm(a) * vec32.Norm(b)

	return (aDotB / normAB)
}

func (O *TableGraph) regenLevels() {
	O.rootPaths = path.DijkstraFrom(O.root, O.DirectedGraph)
}

func (O *TableGraph) getLevel(s graph.Node) float64 {
	_, weight := O.rootPaths.To(s.ID())
	return weight
}

func (O *TableGraph) getChildren(s graph.Node) graph.Nodes {
	return O.From(s.ID())
}

func (O *TableGraph) getParents(s graph.Node) graph.Nodes {
	return O.To(s.ID())
}

// Equation (6) from the paper
// Note that this is not quite the same, since we eliminate equation 5 since vectors are computed at the table level
func (O *TableGraph) getOrganizationEffectiveness() float64 {
	var out float64 = 0
	for _, j := range O.leafNodes {
		var prob = O.getStateQueryProbability(j, j.vector)
		out += prob
	}
	return out / float64(len(O.leafNodes))
}

// Equation (10) from the paper
func (O *TableGraph) getStateReachabilityProbability(s graph.Node) float64 {
	var out float64 = 0
	for _, T := range O.leafNodes {
		out = out + O.getStateQueryProbability(s, T.vector)
	}
	s.(*Node).cachedReachibility = out / float64(len(O.leafNodes))
	return s.(*Node).cachedReachibility
}

// Equation (4) From the paper
// TODO: Investigate a better way to do this perhaps using paths API
func (O *TableGraph) getStateQueryProbability(s graph.Node, X []float32) float64 {
	parents := O.getParents(s)
	var out float64 = 0

	for parents.Next() {
		var p = parents.Node()
		var stateTransProb = O.getStateTransitionProbability(s, p, X)
		var parQueryProb float64

		if p != O.root {
			parQueryProb = O.getStateQueryProbability(p, X)
		} else {
			parQueryProb = 1
		}

		out += stateTransProb * parQueryProb
	}

	return out
}

// Equation (1) From the paper
func (O *TableGraph) getStateTransitionProbability(c graph.Node, s graph.Node, X []float32) float64 {
	nc := c.(*Node)
	ns := s.(*Node)
	eGammaChildrenS := math.Exp(O.config.gamma / float64(O.getChildren(s).Len()))
	var divisor float64 = 0
	children := O.getChildren(s)
	for children.Next() {
		var curr = children.Node().(*Node)
		var sim = similarity(curr.vector, X)
		divisor += math.Pow(eGammaChildrenS, float64(sim))
	}

	return math.Pow(eGammaChildrenS, float64(similarity(nc.vector, ns.vector))) / divisor
}

// Use priority queue to get the least reachable nodes at a given level
// It implements the container/heap interface.
type ReachabilityPriorityQueue []*Node

func (pq ReachabilityPriorityQueue) Len() int { return len(pq) }
func (pq ReachabilityPriorityQueue) Less(i, j int) bool {
	return pq[i].cachedReachibility < pq[j].cachedReachibility
}
func (pq ReachabilityPriorityQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }

func (pq *ReachabilityPriorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*Node))
}

func (pq *ReachabilityPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(*pq)
	item := old[n-1]
	old[n-1] = nil
	*pq = old[:n-1]
	return item
}

func (O *TableGraph) nodeArray() []*Node {
	var out []*Node
	for it := O.Nodes(); it.Next(); {
		out = append(out, it.Node().(*Node))
	}
	return out
}

// Wrapper around GoNum's implementation
func (O *TableGraph) CopyOrganization() *TableGraph {
	out := &TableGraph{simple.NewDirectedGraph(), O.config, O.root, path.Shortest{}, O.leafNodes}
	graph.Copy(out, O)

	return out
}

func (O *TableGraph) deleteParent(s graph.Node, idx *index) error {
	parents := O.getParents(s) // Get the parents of the input node
	reachability := math.Inf(1)
	var lowestNode graph.Node // Find the least reachable parent
	for parents.Next() {
		var nodeReach = O.getStateReachabilityProbability(parents.Node())
		if nodeReach < reachability {
			reachability = nodeReach
			lowestNode = parents.Node()
		}
	}

	children := O.getChildren(lowestNode) // Find the children of the least reachable parent

	lowestParents := O.getParents(s) // Get the parents of the least reachable parent

	for children.Next() { // Connect each of the children of the least reachable parent (including s) to each of the parents of the least reachable parent of s
		for lowestParents.Next() {
			O.NewEdge(children.Node(), lowestParents.Node())
		}
		O.RemoveEdge(children.Node().ID(), lowestNode.ID())
		lowestParents.Reset()
	}

	// Remove the least reachable parent of s.
	idx.remove(ToDSNode(lowestNode))
	O.RemoveNode(lowestNode.ID())

	return nil
}

func (O *TableGraph) addParent(s graph.Node, idx *index) error {
	_, simNodes, err := idx.idx.Search(s.(*Node).vector, 600) // Search over 10 percent of the nodes. This probably will need to be tuned, but this whole section does
	if err != nil {
		return err
	}
	for i := range simNodes {
		if O.getLevel(O.Node(simNodes[i])) == O.getLevel(s)-1 {
			O.NewEdge(s, O.Node(simNodes[i]))
			return nil
		}
	}
	return nil
}

func (O *TableGraph) chooseApplyOperation(s graph.Node, idx *index) *TableGraph {
	op := O.CopyOrganization()
	if op.getStateReachabilityProbability(s) >= op.config.threshold {
		op.addParent(s, idx)
	} else {
		op.deleteParent(s, idx)
	}

	return op
}

func (O *TableGraph) chooseOperableState(pq *ReachabilityPriorityQueue) graph.Node {
	return pq.Pop().(graph.Node)
}

func (O *TableGraph) terminate() bool {
	return false
}

func (O *TableGraph) accept(Op *TableGraph, p float64) (*TableGraph, float64) {
	var Pp = Op.getOrganizationEffectiveness()
	if Pp > p {
		return Op, Pp
	}
	return O, p
}

func (O *TableGraph) organize() (*TableGraph, error) {
	idx, err := buildIndex(O)
	if err != nil {
		return nil, err
	}

	var pq ReachabilityPriorityQueue = O.nodeArray()
	heap.Init(&pq)

	var p = O.getOrganizationEffectiveness()
	for !O.terminate() {
		var s = O.chooseOperableState(&pq)
		var Op = O.chooseApplyOperation(s, idx) // O is in the index passed
		O, p = O.accept(Op, p)
	}

	return O, nil
}

func (O *TableGraph) toVisualizer() {
	data, err := dot.Marshal(O, "Organization", "", "")
	if err != nil {
		fmt.Println(err)
	}
	err = ioutil.WriteFile("/tmp/graph", data, 0644)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Wrote graph data")
}
