package store

import (
    "log"
    "path/filepath"
)

//fetch files in directory
func FetchFiles(src_pattern string) []string {
    files, err := filepath.Glob(src_pattern)
    if err != nil {
        log.Fatalln(err)
    }
    return files
}