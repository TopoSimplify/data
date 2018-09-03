package store

import (
	"log"
	"path/filepath"
)

// fetch files in directory
func FetchFiles(srcPattern string) []string {
	files, err := filepath.Glob(srcPattern)
	if err != nil {
		log.Fatalln(err)
	}
	return files
}
