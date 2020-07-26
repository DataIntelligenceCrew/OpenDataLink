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
		if err := InsertName(indexBuilder, fastText, &v); err != nil {
			return err
		}

		if err := InsertDescription(indexBuilder, fastText, &v); err != nil {
			return err
		}

		if err := InsertCategories(indexBuilder, fastText, &v); err != nil {
			return err
		}

		if err := InsertTags(indexBuilder, fastText, &v); err != nil {
			return err
		}
	}

	return nil
}

// InsertName adds Metadata.Name to index
func InsertName(indexBuilder IndexBuilder, fastText *fasttext.FastText, metadata *database.Metadata) error {
	nameEmbeddingVector, err := metadata.NameEmbeddingVector(fastText)
	if err != nil {
		return err
	}
	if nameEmbeddingVector != nil {
		indexBuilder.Insert(nameEmbeddingVector, metadata.Name)
	}
	return nil
}

// InsertDescription adds Metadata.Description to index
func InsertDescription(indexBuilder IndexBuilder, fastText *fasttext.FastText, metadata *database.Metadata) error {
	descriptionEmbeddingVectors, err :=
		metadata.DescriptionEmbeddingVectors(fastText)
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
func InsertCategories(indexBuilder IndexBuilder, fastText *fasttext.FastText, metadata *database.Metadata) error {
	categoriesEmbeddingVectors, err :=
		metadata.CategoriesEmbeddingVectors(fastText)
	if err != nil {
		return err
	}
	if categoriesEmbeddingVectors != nil {
		indexBuilder.InsertZip(&categoriesEmbeddingVectors, &metadata.Categories)
	}
	return nil
}

// InsertTags adds Metadata.Tags to index
func InsertTags(indexBuilder IndexBuilder, fastText *fasttext.FastText, metadata *database.Metadata) error {
	tagsEmbeddingVectors, err := metadata.TagsEmbeddingVectors(fastText)
	if err != nil {
		return err
	}
	if tagsEmbeddingVectors != nil {
		indexBuilder.InsertZip(&tagsEmbeddingVectors, &metadata.Tags)
	}
	return nil
}
