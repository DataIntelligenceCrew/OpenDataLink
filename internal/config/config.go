package config

import "os"

func DatabasePath() string {
	path := os.Getenv("OPENDATALINK_DB")
	if path == "" {
		return "opendatalink.sqlite"
	}
	return path
}

func FasttextPath() string {
	path := os.Getenv("FASTTEXT_DB")
	if path == "" {
		return "fasttext.sqlite"
	}
	return path
}
