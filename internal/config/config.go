package config

import "os"

// DatabasePath returns the path to the Open Data Link database.
// The path is "opendatalink.sqlite", or the contents of the OPENDATALINK_DB
// environment variable if it is set.
func DatabasePath() string {
	if path := os.Getenv("OPENDATALINK_DB"); path != "" {
		return path
	}
	return "opendatalink.sqlite"
}

// FasttextPath returns the path to the fastText database.
// The path is "fasttext.sqlite", or the contents of the FASTTEXT_DB environment
// variable if it is set.
func FasttextPath() string {
	if path := os.Getenv("FASTTEXT_DB"); path != "" {
		return path
	}
	return "fasttext.sqlite"
}
