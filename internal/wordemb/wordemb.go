// Package wordemb creates embedding vectors for text by averaging word vectors.
package wordemb

import (
	"errors"
	"regexp"
	"strings"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/vec32"
	"github.com/ekzhu/go-fasttext"
)

// ErrNoEmb is returned by Vector when none of the input words have an
// embedding.
var ErrNoEmb = errors.New("no embeddings found for input words")

var wordSepRe = regexp.MustCompile(`\W+`)

// Lucene stop words list.
var stopwords = map[string]bool{
	"a":     true,
	"an":    true,
	"and":   true,
	"are":   true,
	"as":    true,
	"at":    true,
	"be":    true,
	"but":   true,
	"by":    true,
	"for":   true,
	"if":    true,
	"in":    true,
	"into":  true,
	"is":    true,
	"it":    true,
	"no":    true,
	"not":   true,
	"of":    true,
	"on":    true,
	"or":    true,
	"such":  true,
	"that":  true,
	"the":   true,
	"their": true,
	"then":  true,
	"there": true,
	"these": true,
	"they":  true,
	"this":  true,
	"to":    true,
	"was":   true,
	"will":  true,
	"with":  true,
}

// Vector creates an embedding vector for the given text by averaging the
// fastText vectors of the words.
//
// Returns a zero vector and ErrNoEmb if none of the input words are found in
// the FastText DB.
func Vector(ft *fasttext.FastText, text []string) ([]float32, error) {
	vec := make([]float32, fasttext.Dim)
	foundEmb := false

	for _, words := range text {
		for _, word := range wordSepRe.Split(words, -1) {
			if stopwords[strings.ToLower(word)] {
				continue
			}
			emb, err := ft.GetEmb(word)
			if err != nil {
				if err == fasttext.ErrNoEmbFound {
					continue
				}
				return nil, err
			}
			foundEmb = true
			vec32.Normalize(emb)
			vec32.Add(vec, emb)
		}
	}
	vec32.Scale(vec, 1/float32(len(vec)))
	vec32.Normalize(vec)

	if !foundEmb {
		return vec, ErrNoEmb
	}
	return vec, nil
}
