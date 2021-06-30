package wordparser

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func allocateParser(tb testing.TB) *WordParser {
	parser, err := New("/localdisk2/opendatalink/fasttext.sqlite")
	if err != nil {
		tb.Fatal(err)
	}

	return parser
}

func BenchmarkWordParserCreation(b *testing.B) {
	allocateParser(b)
}

func BenchmarkWordParserSearch(b *testing.B) {
	b.StopTimer()
	v := make([]float32, 300)
	parser := allocateParser(b)
	b.StartTimer()
	b.ResetTimer()

	_, err := parser.Search(v)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("Faiss Index Size: %v", parser.index.Ntotal())
}
