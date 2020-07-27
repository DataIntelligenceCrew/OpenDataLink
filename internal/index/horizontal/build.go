package horizontal

import (
	"errors"
	"log"
	"os"

	"opendatalink/internal/database"

	"github.com/fnargesian/simhash-lsh"
	"github.com/justinfargnoli/go-fasttext"
)

const dimensionCount = fasttext.Dim
const hashTableCount = 1
const hashValuePerHashTableCount = 1

func fastTextPath() (string, error) {
	path := os.Getenv("FAST_TEXT_DB")
	if path == "" {
		return "", errors.New("'FAST_TEXT_DB' enviroment variable is not set")
	}
	return path, nil
}

// BuildMetadataIndex builds a LSH index using github.com/fnargesian/simhash-lsh
func BuildMetadataIndex(db *database.DB) (Index, error) {
	indexBuilder := NewIndexBuilder(dimensionCount, hashTableCount, hashValuePerHashTableCount)

	path, err := fastTextPath()
	if err != nil {
		return Index{}, nil
	}
	fastText := fasttext.New(path)
	defer func() {
		if err := fastText.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	metadataIterator, err := db.NewMetadataIterator()
	if err != nil {
		return Index{}, err
	}
	for metadataIterator.HasNext() {
		metadata, err := metadataIterator.Row()
		if err != nil {
			return Index{}, err
		}
		if err := indexBuilder.InsertMetadata(metadata, fastText); err != nil {
			return Index{}, nil
		}
	}
	if err := metadataIterator.End(); err != nil {
		return Index{}, nil
	}

	return indexBuilder.ToIndex(), nil
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
func (indexBuilder IndexBuilder) Insert(point Point) {
	indexBuilder.index.Insert(point.EmbeddingVector, point.ID())
}

// InsertZip zips the embeddingVectors and IDs array into a one dimensional
// array of (embeddingVector []float64, ID string) tuples which are then added
// to the index
func (indexBuilder IndexBuilder) InsertZip(embeddingVectors *[][]float64, datasetID string, IDs *[]string) {
	if len(*embeddingVectors) != len(*IDs) {
		log.Fatal("len(embeddingVectors) != len(IDs)")
	}

	for i := range *embeddingVectors {
		indexBuilder.Insert(Point{(*embeddingVectors)[i], datasetID, (*IDs)[i]})
	}
}

// Point is a point in the simhashlsh.CosineLsh Index
type Point struct {
	EmbeddingVector []float64
	DatasetID       string
	Value           string
}

// ID returns the ID to be passed to the third parameter of Index.Insert()
func (point *Point) ID() string {
	return point.DatasetID + point.Value
}

// InsertMetadataRows adds metadataRows to a simhashlsh.CosineLsh index
func (indexBuilder IndexBuilder) InsertMetadataRows(metadataRows *[]database.Metadata) error {
	fastText := fasttext.New(os.Getenv("FAST_TEXT_DB"))
	defer func() {
		if err := fastText.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	for _, v := range *metadataRows {
		indexBuilder.InsertMetadata(&v, fastText)
	}

	return nil
}

// InsertMetadata adds one row of the metadata to the index
func (indexBuilder IndexBuilder) InsertMetadata(metadata *database.Metadata, fastText *fasttext.FastText) error {
	if err := indexBuilder.InsertName(fastText, metadata); err != nil {
		return err
	}
	if err := indexBuilder.InsertDescription(fastText, metadata); err != nil {
		return err
	}
	if err := indexBuilder.InsertCategories(fastText, metadata); err != nil {
		return err
	}
	if err := indexBuilder.InsertTags(fastText, metadata); err != nil {
		return err
	}
	return nil
}

// InsertName adds Metadata.Name to index
func (indexBuilder IndexBuilder) InsertName(fastText *fasttext.FastText, metadata *database.Metadata) error {
	nameEmbeddingVector, err := NameEmbeddingVector(metadata, fastText)
	if err != nil {
		return err
	}
	if nameEmbeddingVector != nil {
		indexBuilder.Insert(Point{nameEmbeddingVector, metadata.DatasetID, metadata.Name})
	}
	return nil
}

// InsertDescription adds Metadata.Description to index
func (indexBuilder IndexBuilder) InsertDescription(fastText *fasttext.FastText, metadata *database.Metadata) error {
	descriptionEmbeddingVectors, err :=
		DescriptionEmbeddingVectors(metadata, fastText)
	if err != nil {
		return err
	}
	if descriptionEmbeddingVectors != nil {
		descriptionClean := metadata.DescriptionSplit()
		indexBuilder.InsertZip(&descriptionEmbeddingVectors, metadata.DatasetID, &descriptionClean)
	}
	return nil
}

// InsertCategories adds Metadata.Categories to index
func (indexBuilder IndexBuilder) InsertCategories(fastText *fasttext.FastText, metadata *database.Metadata) error {
	categoriesEmbeddingVectors, err :=
		CategoriesEmbeddingVectors(metadata, fastText)
	if err != nil {
		return err
	}
	if categoriesEmbeddingVectors != nil {
		indexBuilder.InsertZip(&categoriesEmbeddingVectors, metadata.DatasetID, &metadata.Categories)
	}
	return nil
}

// InsertTags adds Metadata.Tags to index
func (indexBuilder IndexBuilder) InsertTags(fastText *fasttext.FastText, metadata *database.Metadata) error {
	tagsEmbeddingVectors, err := TagsEmbeddingVectors(metadata, fastText)
	if err != nil {
		return err
	}
	if tagsEmbeddingVectors != nil {
		indexBuilder.InsertZip(&tagsEmbeddingVectors, metadata.DatasetID, &metadata.Tags)
	}
	return nil
}

// NameEmbeddingVector returns the embedding vector which represents
// Metadata.Name
// []float64 == nil when an embedding vector does not exist for Metadata.Name
func NameEmbeddingVector(metadata *database.Metadata, fastText *fasttext.FastText) ([]float64, error) {
	nameSplit := metadata.NameSplit()
	embeddingVector, err := fastText.MultiWordEmbeddingVector(nameSplit)
	if err != nil {
		return nil, err
	}

	return embeddingVector, nil
}

// DescriptionEmbeddingVectors returns an array of embedding vectors which
// represent the words of Metadata.Description
// [][]float64 == nil when an embedding vector does not exist for
// Metadata.Description
func DescriptionEmbeddingVectors(metadata *database.Metadata, fastText *fasttext.FastText) ([][]float64, error) {
	descriptionSplit := metadata.DescriptionSplit()
	var descriptionEmbeddingVector [][]float64
	for _, v := range descriptionSplit {
		wordEmbeddingVector, err := fastText.EmbeddingVector(v)
		if err != nil {
			return nil, err
		}
		descriptionEmbeddingVector =
			append(descriptionEmbeddingVector, wordEmbeddingVector)
	}

	return descriptionEmbeddingVector, nil
}

// AttributionEmbeddingVectors returns an array of embedding vectors which
// represent the words of Metadata.Attribution
// [][]float64 == nil when an embedding vector does not exist for
// Metadata.Description
func AttributionEmbeddingVectors(metadata *database.Metadata, fastText *fasttext.FastText) ([][]float64, error) {
	attributionSplit := metadata.AttributionSplit()
	var attributionEmbeddingVector [][]float64
	for _, v := range attributionSplit {
		wordEmbeddingVector, err := fastText.EmbeddingVector(v)
		if err != nil {
			return nil, err
		}
		attributionEmbeddingVector =
			append(attributionEmbeddingVector, wordEmbeddingVector)
	}

	return attributionEmbeddingVector, nil
}

// CategoriesEmbeddingVectors returns an array of embedding vectors which
// represent the words of Metadata.Categories
// [][]float64 == nil when an embedding vector does not exist for
// Metadata.Description
func CategoriesEmbeddingVectors(metadata *database.Metadata, fastText *fasttext.FastText) ([][]float64, error) {
	var categoriesEmbeddingVector [][]float64
	for _, v := range metadata.Categories {
		wordEmbeddingVector, err := fastText.EmbeddingVector(v)
		if err != nil {
			return nil, err
		}
		categoriesEmbeddingVector =
			append(categoriesEmbeddingVector, wordEmbeddingVector)
	}

	return categoriesEmbeddingVector, nil
}

// TagsEmbeddingVectors returns an array of embedding vectors which
// represent the words of Metadata.Tags
// [][]float64 == nil when an embedding vector does not exist for
// Metadata.Description
func TagsEmbeddingVectors(metadata *database.Metadata, fastText *fasttext.FastText) ([][]float64, error) {
	var tagsEmbeddingVector [][]float64
	for _, v := range metadata.Tags {
		wordEmbeddingVector, err := fastText.EmbeddingVector(v)
		if err != nil {
			return nil, err
		}
		tagsEmbeddingVector = append(tagsEmbeddingVector, wordEmbeddingVector)
	}

	return tagsEmbeddingVector, nil
}
