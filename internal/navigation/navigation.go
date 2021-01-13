package navigation

import (
	"container/heap"
	"fmt"
	"io/ioutil"
	"math"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/vec32"
	"github.com/DataIntelligenceCrew/go-faiss"
	"github.com/ekzhu/go-fasttext"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/encoding/dot"
	"gonum.org/v1/gonum/graph/path"
	"gonum.org/v1/gonum/graph/simple"
)

// Config for an organization
type Config struct {
	Gamma                float64 // The model's gamma parameter, a penalty for a node having too many children
	TerminationThreshold float64 // The threshold below which the learning algorithm stops
	TerminationWindow    int     // The number of prior iterations to account for in terminating
	MaxIters             int     // The node reachability below which we choose to delete a parent instead of adding a parent
}

const embeddingDim = 300

// Node a dataset specific node
type Node struct {
	id                 int64
	cachedReachibility float64
	vector             []float32       // Metadata embedding vector for the datasets
	datasets           map[string]bool // Set of dataset IDs of children
	name               string
	dataset            string
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
		name:               datasetID,
		dataset:            datasetID,
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
	return &Node{id, 0, vec, datasets, "", ""}
}

// TableGraph the custom graph structure for an organization
type TableGraph struct {
	*simple.DirectedGraph
	config    *Config
	root      graph.Node
	rootPaths path.Shortest
	leafNodes []*Node
}

func newGraph(cfg *Config) *TableGraph {
	return &TableGraph{simple.NewDirectedGraph(), cfg, nil, path.Shortest{}, make([]*Node, 0)}
}

// addDatasetNodes creates nodes for the datasets and adds them to the graph.
func (O *TableGraph) addDatasetNodes(db *database.DB, ids []string) error {
	for _, s := range ids {
		// Query returning a single item
		rows, err := db.Query(`
		SELECT dataset_id, emb FROM metadata_vectors WHERE dataset_id=
		'` + s + `';`)
		if err != nil {
			return err
		}
		defer rows.Close()
		rows.Next()
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
	return nil
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

func BuildOrganization(db *database.DB, ft *fasttext.FastText, cfg *Config, ids []string) (*TableGraph, error) {
	g, err := BuildInitialOrg(db, cfg, ids)
	if err != nil {
		return nil, err
	}
	g, err = g.organize()
	if err != nil {
		return nil, err
	}
	if err := g.labelNodes(db, ft); err != nil {
		return nil, err
	}
	return g, nil
}

// buildInitialOrg builds the initial organization of the navigation graph.
//
// The initial organization is a binary tree created by joining the most similar
// pairs of nodes under a parent node.
func BuildInitialOrg(db *database.DB, cfg *Config, ids []string) (*TableGraph, error) {
	// Create nodes for all datasets and add them to graph.
	g := newGraph(cfg)
	if err := g.addDatasetNodes(db, ids); err != nil {
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
	normAB := float32(1) // vec32.Norm(a) * vec32.Norm(b) // We don't need this code because the vectors are already normalized

	return (aDotB / normAB)
}

func (O *TableGraph) regenLevels() {
	O.rootPaths = path.DijkstraFrom(O.root, O.DirectedGraph)
}

func (O *TableGraph) getLevel(s graph.Node) int {
	_, weight := O.rootPaths.To(s.ID())
	return int(weight)
}

func (O *TableGraph) getChildren(s graph.Node) graph.Nodes {
	return O.From(s.ID())
}

func (O *TableGraph) GetChildren(s graph.Node) []*Node {
	out := make([]*Node, 0)
	for iter := O.getChildren(s); iter.Next(); {
		out = append(out, iter.Node().(*Node))
	}

	return out
}

func (O *TableGraph) isLeafNode(s graph.Node) bool {
	return O.getChildren(s).Len() == 0
}

func (O *TableGraph) getParents(s graph.Node) graph.Nodes {
	return O.To(s.ID())
}

func (O *TableGraph) GetParents(s graph.Node) []*Node {
	out := make([]*Node, 0)
	for iter := O.getParents(s); iter.Next(); {
		out = append(out, iter.Node().(*Node))
	}

	return out
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

		if p.ID() != O.root.ID() {
			parQueryProb = O.getStateQueryProbability(p, X)
		} else {
			parQueryProb = 1
		}

		out += stateTransProb * parQueryProb
	}

	return out
}

// Equation (1) From the paper
// The probability of going to a state c from a parent state s
func (O *TableGraph) getStateTransitionProbability(c graph.Node, s graph.Node, X []float32) float64 {
	nc := c.(*Node)
	eGammaChildrenS := math.Exp(O.config.Gamma / float64(O.getChildren(s).Len()))
	var divisor float64 = 0
	children := O.getChildren(s)
	for children.Next() {
		var curr = children.Node().(*Node)
		var sim = similarity(curr.vector, X)
		divisor += math.Pow(eGammaChildrenS, float64(sim))
	}

	return math.Pow(eGammaChildrenS, float64(similarity(nc.vector, X))) / divisor
}

func (O *TableGraph) nodeArray() []*Node {
	var out []*Node
	for it := O.Nodes(); it.Next(); {
		out = append(out, it.Node().(*Node))
	}
	return out
}
func (n *Node) copy() *Node {
	var out *Node
	out = new(Node)
	out.id = n.id
	out.cachedReachibility = n.cachedReachibility
	out.vector = make([]float32, embeddingDim)
	copy(out.vector, n.vector)
	out.datasets = n.datasets
	out.name = n.name
	out.dataset = n.dataset
	// fmt.Println(out.vector)
	return out
}

// Wrapper around GoNum's implementation
func (O *TableGraph) CopyOrganization() *TableGraph {
	out := &TableGraph{simple.NewDirectedGraph(), O.config, O.root.(*Node).copy(), O.rootPaths, make([]*Node, 0)}

	// Deep copy nodes
	for it := O.Nodes(); it.Next(); {
		toAdd := it.Node().(*Node).copy()
		out.AddNode(toAdd)
		if O.isLeafNode(it.Node()) {
			out.leafNodes = append(out.leafNodes, toAdd)
		}
	}

	// Deep copy edges
	for it := O.Edges(); it.Next(); {
		from := it.Edge().From().ID()
		to := it.Edge().To().ID()
		out.SetEdge(out.NewEdge(out.Node(from), out.Node(to)))
	}
	return out
}

func (O *TableGraph) SetRootName(name string) {
	O.GetRootNode().(*Node).name = name
}

func (O *TableGraph) getSiblings(s graph.Node) []graph.Node {
	var out []graph.Node
	for it := O.getParents(s); it.Next(); {
		out = append(out, graph.NodesOf(O.getChildren(it.Node()))...)
	}
	return out
}

func (O *TableGraph) eliminateNode(s graph.Node) {
	children := O.getChildren(s)
	parents := O.getParents(s)

	for parents.Next() {
		for children.Next() {
			O.addEdge(parents.Node(), children.Node())
		}
		children.Reset()
	}

	O.RemoveNode(s.ID())
}

/* update_vector recursively updates the topic vector of a state based on it's domain
 *
 */
func (O *TableGraph) update_vector(s *Node) []float32 {
	total := make([]float32, embeddingDim)
	var n int = 0
	for it := O.getChildren(s); it.Next(); {
		if O.getChildren(it.Node()).Len() != 0 {
			vec32.Add(total, O.update_vector(it.Node().(*Node)))
		} else {
			vec32.Add(total, it.Node().(*Node).vector)
		}
		n++
	}
	//vec32.Scale(total, 1/float32(n))
	vec32.Normalize(total)
	s.vector = total
	//fmt.Println(s.vector)
	return total
}

func (O *TableGraph) deleteParent(s int64) ([]graph.Node, error) {
	node := O.Node(s)
	parents := O.getParents(node) // Get the parents of the input node
	reachability := math.Inf(1)
	var lowestNode graph.Node // Find the least reachable parent

	for parents.Next() {
		var nodeReach = O.getStateReachabilityProbability(parents.Node())
		if nodeReach < reachability {
			reachability = nodeReach
			lowestNode = parents.Node()
		}
	}

	if lowestNode == nil {
		return nil, nil
	}

	var removedNodes []graph.Node

	for _, s := range O.getSiblings(lowestNode) {
		if !O.isLeafNode(s) {
			O.eliminateNode(s)
			removedNodes = append(removedNodes, s)
		}
	}

	O.eliminateNode(lowestNode)
	removedNodes = append(removedNodes, lowestNode)

	O.update_vector(O.root.(*Node))
	// fmt.Printf("Del Parent Effectiveness: %v\n", O.getOrganizationEffectiveness())
	// O.toVisualizer("/tmp/last_del_op.dot")

	return removedNodes, nil
}

func (O *TableGraph) addEdge(from graph.Node, to graph.Node) bool {
	if !O.isLeafNode(from) && !O.HasEdgeFromTo(from.ID(), to.ID()) {
		// vec32.Scale(from.(*Node).vector, float32(O.getChildren(from).Len()))
		O.SetEdge(O.NewEdge(from, to))
		// vec32.Add(from.(*Node).vector, to.(*Node).vector)
		// vec32.Scale(from.(*Node).vector, 1/float32(O.getChildren(from).Len()))
		// vec32.Normalize(from.(*Node).vector)
		// fmt.Printf("Added edge from %v (level %v) to %v (level %v)\n", from.ID(), O.getLevel(from), to.ID(), O.getLevel(to))
		return true
	}
	return false
}

func (O *TableGraph) addParent(s int64, pq *ReachabilityPriorityQueue) error {
	node := O.Node(s)
	// level := O.getLevel(node)
	// parents := pq
	var bestParent *Node
	for pq.HasNext() {
		bestParent = pq.Pop().(*Node)
	}
	O.addEdge(bestParent, node)

	O.update_vector(O.root.(*Node))
	// O.toVisualizer("/tmp/last_add_op.dot")
	// fmt.Printf("Add Parent Effectiveness: %v\n", O.getOrganizationEffectiveness())

	return nil
}

func (O *TableGraph) applyDelOperation(s graph.Node, lvl int) *TableGraph {
	opDel := O.CopyOrganization()
	opDel.deleteParent(s.ID())
	return opDel
}

// TODO: Make this more intelligent.
func (O *TableGraph) chooseApplyOperation(s graph.Node, level int) *TableGraph {
	opAdd := O.CopyOrganization()
	opDel := O.CopyOrganization()

	pq := opAdd.buildPriorityQueue()

	if level >= 1 && len(pq[level-1]) >= 2 {
		opAdd.addParent(s.ID(), &pq[level-1])
	}
	opDel.deleteParent(s.ID())
	if opAdd.getOrganizationEffectiveness() > opDel.getOrganizationEffectiveness() {
		return opAdd
	} else {
		return opDel
	}
}

func (O *TableGraph) chooseOperableState(pq *ReachabilityPriorityQueue, t *terminationMonitor) graph.Node {
	node := pq.Pop().(graph.Node)
	var out graph.Node
	if t.isHung(int(node.ID())) {
		out = pq.Pop().(graph.Node)
		pq.Push(node)
	} else {
		out = node
	}
	if O.Node(node.ID()) != nil {
		return out
	}
	return O.chooseOperableState(pq, t)
}

type terminationMonitor struct {
	window     []float64
	nodeWindow []int
	cursor     int
	iterations int
}

func (t *terminationMonitor) isHung(s int) bool {
	var out int
	for i := range t.nodeWindow {
		out += t.nodeWindow[i]
	}

	return (out / len(t.nodeWindow)) == s
}

func (t *terminationMonitor) updateWindow(s float64, i int) {
	t.window[t.cursor] = s
	t.nodeWindow[t.cursor] = i
	t.cursor = (t.cursor + 1) % len(t.window)
	t.iterations++
}

func (t *terminationMonitor) calcAvg() float64 {
	var out float64

	for i := range t.window {
		out += t.window[i]
	}
	divisor := float64(len(t.window)) // math.Min(float64(t.iterations), float64(len(t.window)))

	return out / divisor
}

func (O *TableGraph) terminate(t *terminationMonitor, pp float64) bool {
	if t.calcAvg() == 0.0 {
		// fmt.Println(t.window)
		return false
	}
	pctchange := (pp - t.calcAvg()) / t.calcAvg()
	fmt.Printf("iterations: %v\n", t.iterations)
	fmt.Printf("\tt avg: %.10e\n", t.calcAvg())
	fmt.Printf("\tDelta Org effectiveness: %v\n", pp-t.window[t.cursor])
	fmt.Printf("\tnew org effectiveness: %.10e\n", pp)
	fmt.Printf("\tPercent Change from P: %v\n", pctchange)
	return (pctchange < O.config.TerminationThreshold || t.iterations > O.config.MaxIters)
}

func (O *TableGraph) accept(Op *TableGraph) (*TableGraph, float64) {
	var Pp = Op.getOrganizationEffectiveness()
	var p = O.getOrganizationEffectiveness()
	// fmt.Printf("p: %v\n", p)
	// fmt.Printf("Pp: %v\n", Pp)
	// fmt.Printf("Delta Reachability: %v\n", p-Pp)
	if Pp > p {
		return Op, Pp
	}
	return O, p
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

func (pq *ReachabilityPriorityQueue) HasNext() bool {
	return len(*pq) > 0
}

func (O *TableGraph) buildPriorityQueue() []ReachabilityPriorityQueue {
	var out = make([]ReachabilityPriorityQueue, 100)

	for it := O.Nodes(); it.Next(); {
		if it.Node().ID() != O.root.ID() { // We don't want the root node in the priority queue, since we can't operate on it
			level := O.getLevel(it.Node()) - 1 // Put nodes in level 1 at pos 0, and so on
			if len(out) < level {              // If the map is not long enough, we allocate more space
				out = append(out, make([]*Node, 0))
			}
			out[level] = append(out[level], it.Node().(*Node)) // Add the node to the proper array
			heap.Init(&out[level])                             // Init the PriorityQueue for the level
		}
	}

	return out
}

func (O *TableGraph) organize() (*TableGraph, error) {
	t := &terminationMonitor{make([]float64, O.config.TerminationWindow), make([]int, O.config.TerminationWindow), 0, 0}
	// idx, err := buildIndex(O)
	// if err != nil {
	// 	return nil, err
	// }

	var pq []ReachabilityPriorityQueue = O.buildPriorityQueue()

	var p = O.getOrganizationEffectiveness()
	for i := 0; i < 2; i++ {
		fmt.Println(t.window)
		for level := range pq {
			lvl := level
			for pq[lvl].HasNext() {
				s := pq[lvl].Pop().(*Node)
				var Op = O.applyDelOperation(s, lvl)
				O, p = O.accept(Op)
			}
		}
		pq = O.buildPriorityQueue()
	}

	for !O.terminate(t, p) {
		p = O.getOrganizationEffectiveness()
		for level := range pq {
			lvl := level //len(pq) - level - 1 // For reverse order
			for pq[lvl].HasNext() {
				s := pq[lvl].Pop().(*Node)
				var Op = O.chooseApplyOperation(s, lvl)
				O, p = O.accept(Op)
				t.updateWindow(O.getOrganizationEffectiveness(), int(s.ID()))
			}
			pq = O.buildPriorityQueue()
			if O.terminate(t, p) {
				break
			}
		}
		O.regenLevels()
		pq = O.buildPriorityQueue()
	}

	return O, nil
}

func (n *Node) DOTID() string {
	return "\"" + n.name + "\""
}

func (O *TableGraph) ToVisualizer(path string) {
	data, err := dot.Marshal(O, "Organization", "", "")
	if err != nil {
		fmt.Println(err)
	}
	err = ioutil.WriteFile(path, data, 0644)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Wrote graph data")
}
