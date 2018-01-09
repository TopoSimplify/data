package store

import (
	"time"
	"encoding/json"
	"github.com/boltdb/bolt"
)
//imonum, mmsi, status, station, speed, long, lat, course, heading, time, type
type MTraffic struct {
	IMOnum  int         `json:"imonum"  toml:"imonum"`
	MMSI    int         `json:"mmsi"    toml:"mmsi"`
	Status  int         `json:"status"  toml:"status"`
	Station string      `json:"station" toml:"station"`
	Speed   float64     `json:"speed"   toml:"speed"`
	X       float64     `json:"x"       toml:"x"`
	Y       float64     `json:"y"       toml:"y"`
	Course  float64     `json:"course"  toml:"course"`
	Heading float64     `json:"heading" toml:"heading"`
	Time    time.Time   `json:"time"    toml:"time"`
	Type    int         `json:"type"    toml:"type"`
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
