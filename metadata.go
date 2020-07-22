package main

import (
	"database/sql"
	"log"
	"strings"

	"github.com/fnargesian/simhash-lsh"
	"github.com/justinfargnoli/go-fasttext"
)

// Metadata of a dataset ID, namely Metadata.ID
// The data is retrieved form the 'Metadata' table in 'opendatalink.sqlite'
type Metadata struct {
	ID           *string // four-by-four (e.g. "ad4f-f5gs")
	Name         *string
	Description  *string
	Attribution  *string
	ContactEmail *string
	UpdatedAt    *string
	Categories   *string // Comma-separated tags
	Tags         *string // Comma-separated tags
	Permalink    *string // Permant link to the dataset
}

// MetadataRows retreives all data in the Metadata table.
func MetadataRows(db *sql.DB) (*[]Metadata, error) {
	rows, err := db.Query("SELECT * from Metadata;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metadataRows []Metadata
	for isNext := rows.Next(); isNext; isNext = rows.Next() {
		var metadata Metadata
		if err := rows.Scan(metadata.ID, metadata.Name,
			metadata.Description, metadata.Attribution, metadata.ContactEmail,
			metadata.UpdatedAt, metadata.Categories, metadata.Tags,
			metadata.Permalink); err != nil {
			return nil, err
		}
		metadataRows = append(metadataRows, metadata)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &metadataRows, nil
}

// IsEmpty sets the string to nil if the semantics of the string indicte
// that the string is nil.
func IsEmpty(attribute *string) bool {
	if *attribute == "" || *attribute == "null" || *attribute == "Null" ||
		*attribute == "NULL" {
		return true
	}
	return false
}

// NameClean returns a cleaned version of Metadata.Name()
// If Metadata.NameClean() == nil, then Metadata.Name() is empty.
func (metadata *Metadata) NameClean() []string {
	if IsEmpty(metadata.Name) {
		return nil
	}

	return strings.Fields(*metadata.Name)
}

// NameEmbeddingVector returns the embedding vector which represents
// Metadata.Name
// []float64 == nil when an embedding vector does not exist for Metadata.Name
func (metadata *Metadata) NameEmbeddingVector(fastText *fasttext.FastText) ([]float64, error) {
	nameClean := metadata.NameClean()
	if nameClean == nil {
		return nil, nil
	}

	embeddingVector, err := fastText.MultiWordEmbeddingVector(nameClean)
	if err != nil {
		return nil, err
	}

	return embeddingVector, nil
}

// DescriptionClean returns a cleaned version of Metadata.Description()
// If Metadata.DescriptionClean() == nil, then Metadata.Description() is empty.
func (metadata *Metadata) DescriptionClean() []string {
	if IsEmpty(metadata.Description) {
		return nil
	}

	return strings.Fields(*metadata.Description)
}

// DescriptionEmbeddingVectors returns an array of embedding vectors which
// represent the words of Metadata.Description
// [][]float64 == nil when an embedding vector does not exist for
// Metadata.Description
func (metadata *Metadata) DescriptionEmbeddingVectors(fastText *fasttext.FastText) ([][]float64, error) {
	descriptionClean := metadata.DescriptionClean()
	if descriptionClean == nil {
		return nil, nil
	}

	var descriptionEmbeddingVector [][]float64
	for _, v := range descriptionClean {
		wordEmbeddingVector, err := fastText.EmbeddingVector(v)
		if err != nil {
			return nil, err
		}
		descriptionEmbeddingVector =
			append(descriptionEmbeddingVector, wordEmbeddingVector)
	}

	return descriptionEmbeddingVector, nil
}

// AttributionClean returns a cleaned version of Metadata.Attribution()
// If Metadata.AttributionClean() == nil, then Metadata.Attribution() is empty.
func (metadata *Metadata) AttributionClean() []string {
	if IsEmpty(metadata.Attribution) {
		return nil
	}

	return strings.Fields(*metadata.Attribution)
}

// AttributionEmbeddingVectors returns an array of embedding vectors which
// represent the words of Metadata.Attribution
// [][]float64 == nil when an embedding vector does not exist for
// Metadata.Description
func (metadata *Metadata) AttributionEmbeddingVectors(fastText *fasttext.FastText) ([][]float64, error) {
	attributionClean := metadata.AttributionClean()
	if attributionClean == nil {
		return nil, nil
	}

	var attributionEmbeddingVector [][]float64
	for _, v := range attributionClean {
		wordEmbeddingVector, err := fastText.EmbeddingVector(v)
		if err != nil {
			return nil, err
		}
		attributionEmbeddingVector =
			append(attributionEmbeddingVector, wordEmbeddingVector)
	}

	return attributionEmbeddingVector, nil
}

// CategoriesClean returns a cleaned version of Metadata.Categories()
// If Metadata.CategoriesClean() == nil, then Metadata.Categories() is empty.
func (metadata *Metadata) CategoriesClean() []string {
	if IsEmpty(metadata.Categories) {
		return nil
	}

	return strings.Fields(*metadata.Categories)
}

// CategoriesEmbeddingVectors returns an array of embedding vectors which
// represent the words of Metadata.Categories
// [][]float64 == nil when an embedding vector does not exist for
// Metadata.Description
func (metadata *Metadata) CategoriesEmbeddingVectors(fastText *fasttext.FastText) ([][]float64, error) {
	categoriesClean := metadata.CategoriesClean()
	if categoriesClean == nil {
		return nil, nil
	}

	var categoriesEmbeddingVector [][]float64
	for _, v := range categoriesClean {
		wordEmbeddingVector, err := fastText.EmbeddingVector(v)
		if err != nil {
			return nil, err
		}
		categoriesEmbeddingVector =
			append(categoriesEmbeddingVector, wordEmbeddingVector)
	}

	return categoriesEmbeddingVector, nil
}

// TagsClean returns a cleaned version of Metadata.Tags()
// If Metadata.TagsClean() == nil, then Metadata.Tags() is empty.
func (metadata *Metadata) TagsClean() []string {
	if IsEmpty(metadata.Tags) {
		return nil
	}

	return strings.Fields(*metadata.Tags)
}

// TagsEmbeddingVectors returns an array of embedding vectors which
// represent the words of Metadata.Tags
// [][]float64 == nil when an embedding vector does not exist for
// Metadata.Description
func (metadata *Metadata) TagsEmbeddingVectors(fastText *fasttext.FastText) ([][]float64, error) {
	tagsClean := metadata.TagsClean()
	if tagsClean == nil {
		return nil, nil
	}

	var tagsEmbeddingVector [][]float64
	for _, v := range tagsClean {
		wordEmbeddingVector, err := fastText.EmbeddingVector(v)
		if err != nil {
			return nil, err
		}
		tagsEmbeddingVector = append(tagsEmbeddingVector, wordEmbeddingVector)
	}

	return tagsEmbeddingVector, nil
}

// InsertName adds Metadata.Name to index
func InsertName(indexBuilder IndexBuilder, fastText *fasttext.FastText, metadata *Metadata) error {
	nameEmbeddingVector, err := metadata.NameEmbeddingVector(fastText)
	if err != nil {
		return err
	}
	if nameEmbeddingVector != nil {
		indexBuilder.Insert(nameEmbeddingVector, *metadata.Name)
	}
	return nil
}

// InsertDescription adds Metadata.Description to index
func InsertDescription(indexBuilder IndexBuilder, fastText *fasttext.FastText, metadata *Metadata) error {
	descriptionEmbeddingVectors, err :=
		metadata.DescriptionEmbeddingVectors(fastText)
	if err != nil {
		return err
	}
	if descriptionEmbeddingVectors != nil {
		descriptionClean := metadata.DescriptionClean()
		indexBuilder.InsertZip(&descriptionEmbeddingVectors, &descriptionClean)
	}
	return nil
}

// InsertCategories adds Metadata.Categories to index
func InsertCategories(indexBuilder IndexBuilder, fastText *fasttext.FastText, metadata *Metadata) error {
	categoriesEmbeddingVectors, err :=
		metadata.CategoriesEmbeddingVectors(fastText)
	if err != nil {
		return err
	}
	if categoriesEmbeddingVectors != nil {
		categoriesClean := metadata.CategoriesClean()
		indexBuilder.InsertZip(&categoriesEmbeddingVectors, &categoriesClean)
	}
	return nil
}

// InsertTags adds Metadata.Tags to index
func InsertTags(indexBuilder IndexBuilder, fastText *fasttext.FastText, metadata *Metadata) error {
	tagsEmbeddingVectors, err := metadata.TagsEmbeddingVectors(fastText)
	if err != nil {
		return err
	}
	if tagsEmbeddingVectors != nil {
		tagsClean := metadata.TagsClean()
		indexBuilder.InsertZip(&tagsEmbeddingVectors, &tagsClean)
	}
	return nil
}

const dimensionCount = fasttext.Dim
const hashTableCount = 1
const hashValuePerHashTableCount = 1

// InsertMetadata adds metadataRows to a simhashlsh.CosineLsh index
func InsertMetadata(metadataRows *[]Metadata) (Index, error) {
	indexBuilder := NewIndexBuilder(dimensionCount, hashTableCount, hashValuePerHashTableCount)
	fastText := fasttext.New("fast_text.sqlite")

	for _, v := range *metadataRows {
		if err := InsertName(indexBuilder, fastText, &v); err != nil {
			return Index{}, err
		}

		if err := InsertDescription(indexBuilder, fastText, &v); err != nil {
			return Index{}, err
		}

		if err := InsertCategories(indexBuilder, fastText, &v); err != nil {
			return Index{}, err
		}

		if err := InsertTags(indexBuilder, fastText, &v); err != nil {
			return Index{}, err
		}
	}

	return indexBuilder.ToIndex(), nil
}

// BuildMetadataIndex builds a LSH index using github.com/fnargesian/simhash-lsh
func BuildMetadataIndex(db *sql.DB) (Index, error) {
	metadataRows, err := MetadataRows(db)
	if err != nil {
		return Index{}, err
	}

	index, err := InsertMetadata(metadataRows)
	if err != nil {
		return Index{}, err
	}

	return index, nil
}

// IndexBuilder is a write only wrapper of simhashlsh.CosineLsh
type IndexBuilder struct {
	index *simhashlsh.CosineLsh
}

// NewIndexBuilder constructs an IndexBuilder
//
// dimensionCount, hashTableCount, hashValuePerHashTableCount  of
// NewIndexBuilder(dimensionCount, hashTableCount, hashValuePerHashTableCount)
// map to simhash.NewCosinLsh(dim, l m)'s dim, l, and m respectivly
func NewIndexBuilder(dimensionCount, hashTableCount, hashValuePerHashTableCount int) IndexBuilder {
	return IndexBuilder{
		index: simhashlsh.NewCosineLsh(dimensionCount, hashTableCount, hashValuePerHashTableCount),
	}
}

// ToIndex coverts the IndexBuilder to an Index
func (indexBuilder IndexBuilder) ToIndex() Index {
	return Index{
		index: indexBuilder.index,
	}
}

// Insert adds the embeddingVector and id to the index
func (indexBuilder IndexBuilder) Insert(embeddingVector []float64, ID string) {
	indexBuilder.index.Insert(embeddingVector, ID)
}

// InsertZip zips the embeddingVectors and IDs array into a one dimensional
// array of (embeddingVector []float64, ID string) tuples which are then added
// to the index
func (indexBuilder IndexBuilder) InsertZip(embeddingVectors *[][]float64, IDs *[]string) {
	if len(*embeddingVectors) != len(*IDs) {
		log.Fatal("len(embeddingVectors) !=len(IDs)")
	}

	for i := range *embeddingVectors {
		indexBuilder.Insert((*embeddingVectors)[i], (*IDs)[i])
	}
}

// Index is a read only wrapper of simhashlsh.CosineLsh
type Index struct {
	index *simhashlsh.CosineLsh
}

// Query finds the ids of approximate nearest neighbour candidates, in
// un-sorted order, given the query point.
func (i Index) Query(query []float64) []string {
	return i.index.Query(query)
}