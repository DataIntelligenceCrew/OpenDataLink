package navigation

import (
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/OpenDataLink/internal/vec32"
	"github.com/DataIntelligenceCrew/go-faiss"

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
	vec32.Scale(vec, 1/2)
	vec32.Normalize(vec)

	datasets := make(map[string]bool)
	for _, n := range []*node{a, b} {
		for k, v := range n.datasets {
			datasets[k] = v
		}
	}
	return &node{id, vec, datasets}
}

type graph struct {
	*simple.DirectedGraph
}

func newGraph() *graph {
	return &graph{simple.NewDirectedGraph()}
}

// addDatasetNodes creates nodes for the datasets and adds them to the graph.
func (g *graph) addDatasetNodes(db *database.DB) error {
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

// vectors returns the vectors of the nodes in g and the corresponding IDs.
func (g *graph) vectors() (vectors []float32, ids []int64) {
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
	g   *graph
}

func buildIndex(g *graph) (*index, error) {
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

// closestPair returns the pair of nodes that are most similar.
// a and b will not be nil unless there was an error.
func (idx *index) closestPair() (a, b *node, err error) {
	// Query vectors and IDs
	vecs, qids := idx.g.vectors()
	// Result IDs and corresponding cosine similarity
	cos, ids, err := idx.idx.Search(vecs, 1)
	if err != nil {
		return
	}

	var maxCos float32 = -2
	var maxi int
	for i, c := range cos {
		if c > maxCos {
			maxCos = c
			maxi = i
		}
	}
	a = idx.g.Node(qids[maxi]).(*node)
	b = idx.g.Node(ids[maxi]).(*node)
	if a == nil || b == nil {
		panic("a or b is nil")
	}
	return
}

func (idx *index) delete() {
	idx.idx.Delete()
}

// buildInitialOrg builds the initial organization of the navigation graph.
//
// 1. Create nodes for all datasets and add them to graph.
// 2. Create index over the nodes.
// 3a. Query index for closest pair.
// 3b. Remove closest pair of nodes from from index.
// 3c. Created merged node from closest pair and add it to graph with edges to
//     the pair of nodes.
// 3d. Add the merged node to the index.
// 3e. Repeat until no more nodes in index.
func buildInitialOrg(db *database.DB) (*graph, error) {
	g := newGraph()
	if err := g.addDatasetNodes(db); err != nil {
		return nil, err
	}
	index, err := buildIndex(g)
	if err != nil {
		return nil, err
	}
	defer index.delete()

	for index.ntotal() > 1 {
		a, b, err := index.closestPair()
		if err != nil {
			return nil, err
		}
		if err := index.remove(a, b); err != nil {
			return nil, err
		}
		id := g.NewNode().ID()
		node := newMergedNode(id, a, b)
		g.AddNode(node)
		g.SetEdge(g.NewEdge(node, a))
		g.SetEdge(g.NewEdge(node, b))

		if err := index.add(node); err != nil {
			return nil, err
		}
	}
	return g, nil
}
