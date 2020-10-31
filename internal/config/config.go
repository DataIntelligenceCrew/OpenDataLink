package config

import "os"

// DatabasePath returns the path to the Open Data Link database.
// The path is "opendatalink.sqlite", or the contents of the OPENDATALINK_DB
// environment variable if it is set.
func DatabasePath() string {
	path := os.Getenv("OPENDATALINK_DB")
	if path == "" {
		return "opendatalink.sqlite"
	}
	return path
}

// FasttextPath returns the path to the fastText database.
// The path is "fasttext.sqlite", or the contents of the FASTTEXT_DB environment
// variable if it is set.
func FasttextPath() string {
	path := os.Getenv("FASTTEXT_DB")
	if path == "" {
		return "fasttext.sqlite"
	}
	return path
}
