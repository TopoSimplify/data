package store

import (
	"encoding/json"
	"github.com/boltdb/bolt"
	"log"
)

/* mmsi, traj */
type MTraj struct {
	MMSI int            `json:"mmsi"  toml:"mmsi"`
	Traj []*MTraffic    `json:"traj"  toml:"traj"`
}

//creates time as id of bytes
func (mt *MTraj) Id() []byte {
	return ItoB(mt.MMSI)
}

//saves self to a given bucket
func (mt *MTraj) Save(b *bolt.Bucket) error {
	var err error = nil
	v, err := json.Marshal(mt)
	if IsErr(err) {
		return err
	}
	k, err := NextId(b)
	if IsErr(err) {
		log.Fatalln(err)
	}
	if err = Put(b, k, v); IsErr(err) {
		return err
	}

	return nil
}

//Stores a buffer of marrine traffic records
func (store *Store) BulkLoadTrajStorage(mbuffer []*MTraj) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		buckList := make(map[int]*bolt.Bucket)
		var vb *bolt.Bucket
		var err error

		for _, mt := range mbuffer {
			vb = buckList[mt.MMSI]
			if vb == nil {
				vb, err = tx.CreateBucketIfNotExists(mt.Id())
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

//Stores a buffer of marrine traffic records
func (store *Store) TrajKeys() [][]byte {
	keys := make([][]byte, 0)
	store.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		c := tx.Cursor()
		for k, v := c.First(); len(k) != 0 && v == nil; k, v = c.Next() {
			keys = append(keys, k)
		}
		return nil
	})
	return keys
}
