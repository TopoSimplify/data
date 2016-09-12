package store

import (
	"github.com/boltdb/bolt"
	"log"
	"strconv"
)

type Store struct {
	db *bolt.DB
}

func NewStorage(db *bolt.DB) *Store {
	if db == nil {
		log.Fatalln("storage db is not defined")
	}
	return &Store{db:db}
}


//Stores a buffer of marrine traffic records
func (store *Store) BulkLoadStorage(mbuffer []*MTraffic) error {

	return store.db.Update(func(tx *bolt.Tx) error {
		buckList := make(map[int]*bolt.Bucket)
		var vb *bolt.Bucket
		var err error
		for _, mt := range mbuffer {
			vb = buckList[mt.MMSI]
			if vb == nil {
				vb, err = tx.CreateBucketIfNotExists(B(strconv.Itoa(mt.MMSI)))
				if IsErr(err) {
					return err
				}
				buckList[mt.MMSI] = vb
			}
			mt.Save(vb)
			if IsErr(err) {
				return err
			}
		}
		return nil
	})

}

//returns all job buckets
func (self *Store) AllVessels() []int {
	keys := make([]int, 0)
	err :=  self.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		tx.ForEach(func(k []byte, _ *bolt.Bucket) error {
			id, err :=  strconv.Atoi(string(k))
			if err != nil {
				log.Fatalln(err)
			}
			keys = append(keys, id)
			return nil
		})
		return nil
	})
	if err != nil {
		log.Fatalln(err)
	}
	return keys

}


