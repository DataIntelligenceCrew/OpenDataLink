package horizontal

import (
	"log"
	"os"

	"opendatalink/internal/database"

	"github.com/fnargesian/simhash-lsh"
	"github.com/justinfargnoli/go-fasttext"
)

const dimensionCount = fasttext.Dim
const hashTableCount = 1
const hashValuePerHashTableCount = 1

// BuildMetadataIndex builds a LSH index using github.com/fnargesian/simhash-lsh
func BuildMetadataIndex(db *database.DB) (Index, error) {
	metadataRows, err := db.MetadataRows()
	if err != nil {
		return Index{}, err
	}

	indexBuilder := NewIndexBuilder(dimensionCount, hashTableCount, hashValuePerHashTableCount)

	if indexBuilder.InsertMetadata(metadataRows) != nil {
		return Index{}, err
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
func (indexBuilder IndexBuilder) Insert(embeddingVector []float64, ID string) {
	indexBuilder.index.Insert(embeddingVector, ID)
}

// InsertZip zips the embeddingVectors and IDs array into a one dimensional
// array of (embeddingVector []float64, ID string) tuples which are then added
// to the index
func (indexBuilder IndexBuilder) InsertZip(embeddingVectors *[][]float64, IDs *[]string) {
	if len(*embeddingVectors) != len(*IDs) {
		log.Fatal("len(embeddingVectors) != len(IDs)")
	}

	for i := range *embeddingVectors {
		indexBuilder.Insert((*embeddingVectors)[i], (*IDs)[i])
	}
}

// InsertMetadata adds metadataRows to a simhashlsh.CosineLsh index
func (indexBuilder IndexBuilder) InsertMetadata(metadataRows *[]database.Metadata) error {
	fastText := fasttext.New(os.Getenv("FAST_TEXT_DB"))
	defer func() {
		if err := fastText.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	for _, v := range *metadataRows {
		if err := indexBuilder.InsertName(fastText, &v); err != nil {
			return err
		}

		if err := indexBuilder.InsertDescription(fastText, &v); err != nil {
			return err
		}

		if err := indexBuilder.InsertCategories(fastText, &v); err != nil {
			return err
		}

		if err := indexBuilder.InsertTags(fastText, &v); err != nil {
			return err
		}
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
		indexBuilder.Insert(nameEmbeddingVector, metadata.Name)
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
		indexBuilder.InsertZip(&descriptionEmbeddingVectors, &descriptionClean)
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
		indexBuilder.InsertZip(&categoriesEmbeddingVectors, &metadata.Categories)
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
		indexBuilder.InsertZip(&tagsEmbeddingVectors, &metadata.Tags)
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
