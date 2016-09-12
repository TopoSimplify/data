package store

import (
	"encoding/json"
	"time"
	"github.com/boltdb/bolt"
)
/*
   imonum, mmsi, status, station, speed, long, lat, course, heading, time, type
 */
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

