package store

import (
    "encoding/json"
    "github.com/boltdb/bolt"
    "strconv"
)
/*
   imonum, mmsi, status, station, speed, long, lat, course, heading, time, type
 */
type MTraj struct {
    MMSI int            `json:"mmsi"`
    Traj []*MTraffic    `json:"traj"`
}


//creates time as id of bytes
func (mt *MTraj)  Id() []byte {
    return []byte(strconv.Itoa(mt.MMSI))
}

//saves self to a given bucket
func (mt *MTraj) Save(b *bolt.Bucket) error {
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
