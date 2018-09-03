package store

import (
	"encoding/json"
	"github.com/boltdb/bolt"
	"log"
)

type Store struct {
	db *bolt.DB
}

//new storage
func NewStorage(filename string) *Store {
	return &Store{db: openStore(filename)}
}

//close
func (store *Store) Close() {
	if store.db != nil {
		store.db.Close()
	}
}

//View
func (store *Store) View(fn func(tx *bolt.Tx) error) error {
	return store.db.View(fn)
}

//Update
func (store *Store) Update(fn func(tx *bolt.Tx) error) error {
	return store.db.View(fn)
}

//Stores a buffer of marrine traffic records
func (store *Store) BulkLoadMtStorage(mbuffer []*MTraffic) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		buckList := make(map[int]*bolt.Bucket)
		var vb *bolt.Bucket
		var err error
		for _, mt := range mbuffer {
			vb = buckList[mt.MMSI]
			if vb == nil {
				vb, err = tx.CreateBucketIfNotExists(ItoB(mt.MMSI))
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
func (self *Store) AllVessels() [][]byte {
	keys := make([][]byte, 0)

	err := self.db.View(func(tx *bolt.Tx) error {
		// assume bucket exists and has keys
		tx.ForEach(func(k []byte, _ *bolt.Bucket) error {
			key := make([]byte, len(k))
			copy(key, k)
			keys = append(keys, key)
			return nil
		})
		return nil
	})

	if err != nil {
		log.Fatalln(err)
	}
	return keys

}

//returns all job buckets
func (self *Store) AllPings(key []byte) []*Obj {
	var traffic = make([]*Obj, 0)
	err := self.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(key)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v != nil {
				var mt = &MTraffic{}
				if err := json.Unmarshal(v, mt); err != nil {
					return err
				}
				traffic = append(traffic, NewObj(mt))
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalln(err)
	}
	return traffic
}

//open upload db
func openStore(filename string) *bolt.DB {
	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	return db
}
