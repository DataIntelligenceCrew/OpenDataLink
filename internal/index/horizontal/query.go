package horizontal

import "github.com/fnargesian/simhash-lsh"

// Index is a read only wrapper of simhashlsh.CosineLsh
type Index struct {
	index *simhashlsh.CosineLsh
}

// QueryRaw finds the ids of approximate nearest neighbour candidates, in
// un-sorted order, given the query point.
func (i Index) QueryRaw(query []float64) []string {
	return i.index.Query(query)
}

// QueryResult is the deserialized result of an element of Index.Query()
type QueryResult struct {
	DatasetID string
	Value     string
}

// BuildQueryResult builds a QueryResult given a query result in string form
func BuildQueryResult(queryResult string) QueryResult {
	return QueryResult{queryResult[:10], queryResult[10:]}
}

// Query finds the QueryResult's of approximate nearest neighbour candidates, in
// un-sorted order, given the query point.
func (i Index) Query(query []float64) []QueryResult {
	var queryResults []QueryResult
	for _, v := range i.QueryRaw(query) {
		queryResults = append(queryResults, BuildQueryResult(v))
	}
	return queryResults
}
