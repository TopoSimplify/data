package storage

import (
	"time"
	"github.com/boltdb/bolt"
	"encoding/json"
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

type MTraffic struct {
	IMOnum  int        `json:"imonum"`
	MMSI    int        `json:"mmsi"`
	Status  int        `json:"status"`
	Station string     `json:"station"`
	Speed   float64    `json:"speed"`
	X       float64    `json:"x"`
	Y       float64    `json:"y"`
	Course  float64    `json:"course"`
	Heading float64    `json:"heading"`
	Time    time.Time  `json:"time"`
	Type    int        `json:"type"`
}
/*
   'imonum', 'mmsi', 'status', 'station', 'speed',
   'long', 'lat', 'course', 'heading', 'time', 'type'
 */

//creates time as id of bytes
func (mt *MTraffic)  Id() []byte {
	return []byte(mt.Time.Format(time.RFC3339))
}

//saves self to a given bucket
func (mt *MTraffic) Save(b *bolt.Bucket) error {
	var err error = nil
	v, err := json.Marshal(mt)
	if IsErr(err) {
		return err
	}

	if err = Put(b, mt.Id(), v); IsErr(err) {
		return err
	}

	return nil
}


//creates new mtraffic
func NewMTraffic() *MTraffic {
	return &MTraffic{}
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

