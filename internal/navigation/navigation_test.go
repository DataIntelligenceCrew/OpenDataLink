package navigation

import (
	"testing"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/database"
	_ "github.com/mattn/go-sqlite3"
)

func TestInitialOrg(t *testing.T) {
	db, err := database.New("../../opendatalink.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	g, err := buildInitialOrg(db)
	if err != nil {
		t.Fatal(err)
	}
	if it := g.To(g.root.ID()); it.Len() != 0 {
		t.Error("root has an in-edge")
	}
	for it := g.Nodes(); it.Next(); {
		if nchild := g.From(it.Node().ID()).Len(); nchild != 2 && nchild != 0 {
			t.Error("node does not have 2 or 0 children")
		}
	}
}
