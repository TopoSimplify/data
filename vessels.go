package main

import (
    "fmt"
    . "./store"
    "github.com/boltdb/bolt"
    "log"
    "sort"
)

const DBPath = "/home/titus/01/dev/godev/src/simplex/data/db/mtraffic.db"
var mtDB *bolt.DB

func main() {
    OpenStorage()
    defer CloseStorage()
    var mtStore = NewStorage(mtDB)
    var vessels  = mtStore.AllVessels()
    sort.Ints(vessels)
	fmt.Println(vessels[:10])
}



//open upload db
func OpenStorage() {
    db, err := bolt.Open(DBPath, 0600, nil)
    if err != nil {
        log.Fatal(err)
    }
    mtDB = db
}

//close upoad storage
func CloseStorage() {
    if mtDB != nil {
        mtDB.Close()
    }
}


