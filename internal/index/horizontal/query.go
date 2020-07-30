package horizontal

import (
	"sort"
	"strings"

	"github.com/fnargesian/simhash-lsh"
	"github.com/justinfargnoli/go-fasttext"
)

// Index is a read only wrapper of simhashlsh.CosineLsh
type Index struct {
	index    *simhashlsh.CosineLsh
	fastText fasttext.FastText
}

// QueryRaw finds the ids of approximate nearest neighbour candidates, in
// un-sorted order, given the query point.
func (i Index) QueryRaw(query []float64) []string {
	return i.index.Query(query)
}

// queryResult is the deserialized result of an element of Index.Query()
type queryResult struct {
	datasetID string
	value     string
}

// buildQueryResult builds a queryResult given a query result in string form
func buildQueryResult(query string) queryResult {
	return queryResult{query[:10], query[10:]}
}

// queryEmbeddingVector finds the queryResult's of approximate nearest neighbour
// candidates, in un-sorted order, given the query point.
func (i Index) queryEmbeddingVector(query []float64) []queryResult {
	var queryResults []queryResult
	for _, v := range i.QueryRaw(query) {
		queryResults = append(queryResults, buildQueryResult(v))
	}
	return queryResults
}

// queryWord finds the queryResult's of approximate nearest neighbour
// candidates, in un-sorted order, given the query point.
func (i Index) queryWord(word string) ([]queryResult, error) {
	embeddingVector, err := i.fastText.EmbeddingVector(word)
	if err != nil {
		return nil, err
	}
	return i.queryEmbeddingVector(embeddingVector), nil
}

// query returns the datasetsID which are contain semantically similar data
// to the uncleaned query
func (i Index) query(query string) *[]string {
	querySplit := strings.Fields(query)
	var datasetIDs []string
	for _, word := range querySplit {
		wordResults, err := i.queryWord(word)
		if err != nil {
			continue
		}
		for _, v := range wordResults {
			datasetIDs = append(datasetIDs, v.datasetID)
		}
	}
	return &datasetIDs
}

// Search returns a slice of datasetID order by its semantic similarity to the
// query
func (i Index) Search(query string) []string {
	datasetIDCount := toDatasetIDCount(freqMap(i.query(query)))
	sort.Slice(datasetIDCount, func(i, j int) bool {
		return datasetIDCount[i].count < datasetIDCount[j].count
	})
	var datasetIDs []string
	for _, v := range datasetIDCount {
		datasetIDs = append(datasetIDs, v.datasetID)
	}
	return datasetIDs
}

func freqMap(results *[]string) *map[string]int {
	freqMap := make(map[string]int, len(*results))
	for _, datasetID := range *results {
		freqMap[datasetID]++
	}
	return &freqMap
}

type datasetIDCount struct {
	datasetID string
	count     int
}

func toDatasetIDCount(freqMap *map[string]int) []datasetIDCount {
	var datasetIDCounts []datasetIDCount
	for k, v := range *freqMap {
		datasetIDCounts = append(datasetIDCounts, datasetIDCount{k, v})
	}
	return datasetIDCounts
}
