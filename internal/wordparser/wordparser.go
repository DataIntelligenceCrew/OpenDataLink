package wordparser

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	"github.com/DataIntelligenceCrew/go-faiss"
	"github.com/ekzhu/go-fasttext"
)

type WordParser struct {
	labels map[int]string
	index  *faiss.Index
}

func New(dbpath string) (*WordParser, error) {

	idx, err := faiss.IndexFactory(300, "IDMap,Flat", faiss.MetricInnerProduct)
	if err != nil {
		return nil, err
	}

	parser := &WordParser{labels: make(map[int]string), index: idx}

	db, err := database.New(dbpath)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(`
	SELECT word, emb FROM fasttext
	`)
	if err != nil {
		return nil, err
	}
	var ids []int64 = make([]int64, 0)
	var vectors []float32 = make([]float32, 0)
	defer rows.Close()
	var i int64 = 0
	for rows.Next() {
		i = i + 1
		var word string
		var emb []byte

		if err := rows.Scan(&word, &emb); err != nil {
			return nil, err
		}

		vector, _ := bytesToVec(emb, fasttext.ByteOrder)
		vectors = append(vectors, vector...)
		ids = append(ids, i)
		parser.labels[int(i)] = word
	}

	parser.index.AddWithIDs(vectors, ids)

	return parser, nil
}

// Search for word parsed in from the vectors
func (p *WordParser) Search(v []float32) (string, error) {
	_, id, err := p.index.Search(v, 1)
	if err != nil {
		return "", err
	}
	fmt.Println(id)
	return p.labels[int(id[0])], nil

}

// Stolen from go-fasttext/util.go. I tried using go linkname but it didn't work
func bytesToVec(data []byte, order binary.ByteOrder) ([]float32, error) {
	size := len(data) / 4
	vec := make([]float32, size)
	buf := bytes.NewReader(data)
	var v float32
	for i := range vec {
		if err := binary.Read(buf, order, &v); err != nil {
			return nil, err
		}
		vec[i] = v
	}
	return vec, nil
}
