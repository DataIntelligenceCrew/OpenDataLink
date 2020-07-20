package main

import (
	"database/sql"
	"html/template"
	"log"
	"net/http"

	"github.com/ekzhu/lshensemble"
	_ "github.com/mattn/go-sqlite3"
)

const (
	databasePath = "opendatalink.sqlite"
	// Minhash parameters
	mhSeed = 42
	mhSize = 256
	// Containment threshold
	threshold = 0.5
	// Number of LSH Ensemble partitions
	numPart = 8
	// Maximum value for the minhash LSH parameter K
	// (number of hash functions per band).
	maxK = 4
)

func buildIndex(db *sql.DB) (*lshensemble.LshEnsemble, error) {
	var domainRecords []*lshensemble.DomainRecord

	rows, err := db.Query(`
	SELECT column_id, distinct_count, minhash
	FROM column_sketches
	ORDER BY distinct_count
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var columnID string
		var distinctCount int
		var minhash []byte

		if err = rows.Scan(&columnID, &distinctCount, &minhash); err != nil {
			return nil, err
		}
		sig, err := lshensemble.BytesToSig(minhash)
		if err != nil {
			return nil, err
		}
		domainRecords = append(domainRecords, &lshensemble.DomainRecord{
			Key:       columnID,
			Size:      distinctCount,
			Signature: sig,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	index, err := lshensemble.BootstrapLshEnsembleEquiDepth(
		numPart, mhSize, maxK, len(domainRecords), lshensemble.Recs2Chan(domainRecords))
	if err != nil {
		return nil, err
	}
	return index, nil
}

var joinabilityPage = template.Must(template.New("joinable-columns").Parse(`
<html>
<head>
<title>Open Data Link</title>
</head>
<body>
<h2>{{.DatasetName}}</h2>
<h3>Showing joinable tables on <i>{{.ColumnName}}</i></h3>
{{range .Results}}
	<p>
	{{.DatasetName}} &gt; <a href="/joinable-columns?q={{.ColumnID}}">{{.ColumnName}}</a>
	(containment: {{.Containment}})
	</p>
{{else}}
	<p>No joinable tables.</p>
{{end}}
</body>
</html>
`))

func serverError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func main() {
	db, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		log.Fatal(err)
	}
	// Build LSH Ensemble index
	index, err := buildIndex(db)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("built index")
	db.Close()

	http.HandleFunc("/joinable-columns", func(w http.ResponseWriter, req *http.Request) {
		db, err := sql.Open("sqlite3", databasePath)
		if err != nil {
			serverError(w, err)
			return
		}
		defer db.Close()

		qColID := req.FormValue("q")

		sketchSQL, err := db.Prepare(`
		SELECT dataset_id, column_name, distinct_count, minhash
		FROM column_sketches
		WHERE column_id = ?
		`)
		if err != nil {
			serverError(w, err)
			return
		}
		defer sketchSQL.Close()

		var qDatasetID string
		var qColName string
		var qSize int
		var qMinhash []byte

		err = sketchSQL.QueryRow(qColID).Scan(&qDatasetID, &qColName, &qSize, &qMinhash)
		if err != nil {
			if err == sql.ErrNoRows {
				http.NotFound(w, req)
			} else {
				serverError(w, err)
			}
			return
		}

		nameSQL, err := db.Prepare(`SELECT name FROM metadata WHERE dataset_id = ?`)
		if err != nil {
			serverError(w, err)
			return
		}
		defer nameSQL.Close()

		var qDatasetName string
		if err = nameSQL.QueryRow(qDatasetID).Scan(&qDatasetName); err != nil {
			serverError(w, err)
			return
		}

		type result struct {
			DatasetName string
			ColumnID    string
			ColumnName  string
			Containment float64
		}
		data := struct {
			DatasetName string
			ColumnName  string
			Results     []result
		}{
			DatasetName: qDatasetName,
			ColumnName:  qColName,
		}

		qSig, err := lshensemble.BytesToSig(qMinhash)
		if err != nil {
			serverError(w, err)
			return
		}
		done := make(chan struct{})
		defer close(done)
		results := index.Query(qSig, qSize, threshold, done)

		for key := range results {
			colID := key.(string)
			// Don't include query in results
			if colID == qColID {
				continue
			}
			var datasetID string
			var colName string
			var size int
			var minhash []byte
			var datasetName string

			err = sketchSQL.QueryRow(colID).Scan(&datasetID, &colName, &size, &minhash)
			if err != nil {
				serverError(w, err)
				return
			}
			err = nameSQL.QueryRow(datasetID).Scan(&datasetName)
			if err != nil {
				serverError(w, err)
				return
			}
			sig, err := lshensemble.BytesToSig(minhash)
			if err != nil {
				serverError(w, err)
				return
			}
			containment := lshensemble.Containment(qSig, sig, qSize, size)
			if containment < threshold {
				continue
			}
			data.Results = append(data.Results, result{datasetName, colID, colName, containment})
		}
		err = joinabilityPage.Execute(w, data)
		if err != nil {
			serverError(w, err)
		}
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
