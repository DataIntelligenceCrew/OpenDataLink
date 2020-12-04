package navigation

import (
	"container/heap"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/vec32"
	"github.com/DataIntelligenceCrew/go-faiss"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/simple"
)

const embeddingDim = 300

type node struct {
	id       int64
	vector   []float32       // Metadata embedding vector for the datasets
	datasets map[string]bool // Set of dataset IDs of children
}

func (n *node) ID() int64 { return n.id }

func newDatasetNode(id int64, vector []float32, datasetID string) *node {
	return &node{
		id:       id,
		vector:   vector,
		datasets: map[string]bool{datasetID: true},
	}
}

// newMergedNode creates a new merged node from two nodes.
func newMergedNode(id int64, a, b *node) *node {
	vec := make([]float32, embeddingDim)
	vec32.Add(vec, a.vector)
	vec32.Add(vec, b.vector)
	vec32.Scale(vec, 0.5)
	vec32.Normalize(vec)

	datasets := make(map[string]bool)
	for _, n := range []*node{a, b} {
		for k, v := range n.datasets {
			datasets[k] = v
		}
	}
	return &node{id, vec, datasets}
}

type tableGraph struct {
	*simple.DirectedGraph
	root graph.Node
}

func newGraph() *tableGraph {
	return &tableGraph{simple.NewDirectedGraph(), nil}
}

// addDatasetNodes creates nodes for the datasets and adds them to the graph.
func (g *tableGraph) addDatasetNodes(db *database.DB) error {
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
		id := g.NewNode().ID()
		node := newDatasetNode(id, vec, datasetID)
		g.AddNode(node)
	}
	return rows.Err()
}

// addMergedNode creates a new merged node from a and b and adds it to the graph
// with edges to a and b.
func (g *tableGraph) addMergedNode(a, b *node) *node {
	id := g.NewNode().ID()
	node := newMergedNode(id, a, b)
	g.AddNode(node)
	g.SetEdge(g.NewEdge(node, a))
	g.SetEdge(g.NewEdge(node, b))
	return node
}

// vectors returns the vectors of the nodes in g and the corresponding IDs.
func (g *tableGraph) vectors() (vectors []float32, ids []int64) {
	for it := g.Nodes(); it.Next(); {
		node := it.Node().(*node)
		vectors = append(vectors, node.vector...)
		ids = append(ids, node.id)
	}
	return
}

// index is an index over the vectors of nodes in a graph.
type index struct {
	idx *faiss.Index
	g   *tableGraph
}

// nodePair stores the similarity of two nodes.
type nodePair struct {
	a, b   *node
	cosine float32
}

func buildIndex(g *tableGraph) (*index, error) {
	idx, err := faiss.IndexFactory(embeddingDim, "IDMap,Flat", faiss.MetricInnerProduct)
	if err != nil {
		return nil, err
	}
	vecs, ids := g.vectors()

	if err := idx.AddWithIDs(vecs, ids); err != nil {
		return nil, err
	}
	return &index{idx, g}, nil
}

// ntotal returns the number of indexed vectors.
func (idx *index) ntotal() int64 {
	return idx.idx.Ntotal()
}

func (idx *index) add(n *node) error {
	return idx.idx.AddWithIDs(n.vector, []int64{n.id})
}

func (idx *index) remove(nodes ...*node) error {
	var ids []int64
	for _, node := range nodes {
		ids = append(ids, node.ID())
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
func (idx *index) query(n *node) (*nodePair, error) {
	cos, ids, err := idx.idx.Search(n.vector, 1)
	if err != nil {
		return nil, err
	}
	res := idx.g.Node(ids[0]).(*node)

	return &nodePair{n, res, cos[0]}, nil
}

// query_indexed is like query, but if the query node is in the index, the
// result will not be the query itself.
func (idx *index) query_indexed(n *node) (*nodePair, error) {
	cos, ids, err := idx.idx.Search(n.vector, 2)
	if err != nil {
		return nil, err
	}
	// Usually the first result will be the query itself, but the query could be
	// the second result if two datasets have the same embedding vectors.
	// In this case, use the first result.
	id, sim := ids[1], cos[1]
	if id == n.id {
		id = ids[0]
		sim = cos[0]
	}
	// The next line panics if id is -1 (no result).
	res := idx.g.Node(id).(*node)

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
		a := idx.g.Node(qid).(*node)
		id, sim := ids[i*2+1], cos[i*2+1]
		if id == a.id {
			id = ids[i*2]
			sim = cos[i*2]
		}
		b := idx.g.Node(id).(*node)

		results[i] = &nodePair{a, b, sim}
	}
	return results, nil
}

func (idx *index) delete() {
	idx.idx.Delete()
}

// priorityQueue is used to get the closest pair of nodes.
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

// buildInitialOrg builds the initial organization of the navigation graph.
//
// 1. Create nodes for all datasets and add them to graph.
// 2. Create index over the nodes.
// 3. Query index with all nodes, add (query, NN) pairs to priority queue.
// 4a. Pop closest pair of nodes off PQ and remove the nodes from index.
// 4b. Created merged node from closest pair and add it to graph with edges to
//     the pair of nodes.
// 4c. Add the merged node to the index and PQ.
// 4d. Repeat until index is empty.
func buildInitialOrg(db *database.DB) (*tableGraph, error) {
	g := newGraph()
	if err := g.addDatasetNodes(db); err != nil {
		return nil, err
	}
	index, err := buildIndex(g)
	if err != nil {
		return nil, err
	}
	defer index.delete()

	var pq priorityQueue
	pq, err = index.allPairs()
	if err != nil {
		return nil, err
	}

	for {
		heap.Init(&pq)
		pair := heap.Pop(&pq).(*nodePair)

		if err := index.remove(pair.a, pair.b); err != nil {
			return nil, err
		}
		node := g.addMergedNode(pair.a, pair.b)

		if index.ntotal() == 0 {
			g.root = node
			break
		}

		p, err := index.query(node)
		if err != nil {
			return nil, err
		}
		pq = append(pq, p)

		if err := index.add(node); err != nil {
			return nil, err
		}

		// Update PQ
		for i := 0; i < len(pq); i++ {
			p := pq[i]
			// Remove pairs where a is in removed pair.
			// Update pairs where b is in removed pair.
			if p.a == pair.a || p.a == pair.b {
				n := len(pq)
				pq[i] = pq[n-1]
				pq[n-1] = nil
				pq = pq[:n-1]
				i--
			} else if p.b == pair.a || p.b == pair.b {
				np, err := index.query_indexed(p.a)
				if err != nil {
					return nil, err
				}
				pq[i] = np
			}
		}
	}
	return g, nil
}
