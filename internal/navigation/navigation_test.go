package navigation

import (
	"context"
	"testing"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	_ "github.com/mattn/go-sqlite3"
)

func allocateGraph(t testing.TB) (*TableGraph, *database.DB) {
	db, err := database.New("../../opendatalink.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	g, err := BuildInitialOrg(db, &Config{Gamma: 20, TerminationThreshold: 1.25e-15, TerminationWindow: 500, OperationThreshold: 4e-2}, make([]string, 0))
	if err != nil {
		t.Fatal(err)
	}
	return g, db
}

func TestInitialOrg(t *testing.T) {
	g, _ := allocateGraph(t)
	if it := g.To(g.root.ID()); it.Len() != 0 {
		t.Error("root has an in-edge")
	}
	for it := g.Nodes(); it.Next(); {
		if nchild := g.From(it.Node().ID()).Len(); nchild != 2 && nchild != 0 {
			t.Error("node does not have 2 or 0 children")
		}
	}
}

func BenchmarkInitialOrg(b *testing.B) {
	g, db := allocateGraph(b)

	count := db.QueryRowContext(context.Background(), `
	SELECT COUNT(*)
	FROM metadata_vectors
	WHERE dataset_id IN (
		SELECT dataset_id
		FROM metadata
		WHERE categories LIKE '%education%')
	`)
	var out int

	count.Scan(&out)
	b.Logf("Root Node:\nType: %T\nVector Length: %v", g.root, len(ToDSNode(g.root).vector))
	b.Logf("Number of Tables: %v", out)
	b.Logf("Number of Tables (Graph): %v", len(g.leafNodes))
	b.Logf("Number of Nodes: %v", g.Nodes().Len())

	b.Log(GetNodeJSON(g, g.root))

	// g.toVisualizer()
}

func BenchmarkAvgNodeReachability(b *testing.B) {
	g, _ := allocateGraph(b)
	b.ResetTimer()
	var avg float64
	for it := g.Nodes(); it.Next(); {
		prob := g.getStateReachabilityProbability(it.Node())
		avg += prob
	}
	b.Logf("Average reachability: %v", avg/float64(g.Nodes().Len()))
}

func BenchmarkOrganize(b *testing.B) {
	g, _ := allocateGraph(b)
	b.Logf("Initial Organization Effectiveness: %v", g.getOrganizationEffectiveness())
	g.ToVisualizer("pre_optimized.dot")
	b.ResetTimer()
	gprime, err := g.organize()
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	b.Logf("Optimized Organization Effectiveness: %v", gprime.getOrganizationEffectiveness())
	gprime.ToVisualizer("post_optimized.dot")
}

func BenchmarkInitialOrgEffectiveness(b *testing.B) {
	g, _ := allocateGraph(b)
	b.ResetTimer()
	b.Logf("Organization Effectiveness: %v", g.getOrganizationEffectiveness())
}

func BenchmarkLevelSearch(b *testing.B) {
	g, _ := allocateGraph(b)

	b.ResetTimer()
	g.regenLevels()
}
