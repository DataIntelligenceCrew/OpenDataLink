package horizontal

import "github.com/fnargesian/simhash-lsh"

// Index is a read only wrapper of simhashlsh.CosineLsh
type Index struct {
	index *simhashlsh.CosineLsh
}

// Query finds the ids of approximate nearest neighbour candidates, in
// un-sorted order, given the query point.
func (i Index) Query(query []float64) []string {
	return i.index.Query(query)
}
