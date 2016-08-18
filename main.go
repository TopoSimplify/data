package main

import (
    "github.com/boltdb/bolt"
    "log"
    "fmt"
)
const UploadDBPath = "mtraffic.db"

var UploadDB *bolt.DB


func main(){
    OpenStorage()
    defer CloseStorage()

    fmt.Println("...db")
}

//open upload db
func OpenStorage() {
    db, err := bolt.Open(UploadDBPath, 0600, nil)
    if err != nil {
        log.Fatal(err)
    }
    UploadDB = db
}
//close upoad storage
func CloseStorage() {
    if UploadDB != nil {
        UploadDB.Close()
    }
}



