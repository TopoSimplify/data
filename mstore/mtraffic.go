package storage

import (
    "time"
    "github.com/boltdb/bolt"
    "encoding/json"
)

type MTraffic struct {
    IMOnum  int        `json:"imonum"`
    MMSI    int        `json:"mmsi"`
    Status  int        `json:"status"`
    Station string     `json:"station"`
    Speed   float64    `json:"speed"`
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


//creates new mtraffic
func NewMTraffic() *MTraffic {
    return &MTraffic{}
}

//Stores a buffer of marrine traffic records
func (store *Store) NewMTrafficStorage(key int, mbuffer []*MTraffic) error {
    return store.db.Update(func(tx *bolt.Tx) error {
        buckList := make(map[int]*bolt.Bucket)
        var vb *bolt.Bucket
        var err error
        for _, mt := range mbuffer {
            vb = buckList[mt.MMSI]
            if vb == nil {
                vb, err = tx.CreateBucketIfNotExists(B(mt.MMSI))
                if IsErr(err) {
                    return err
                }
            }
            mt.Save(vb)
            if IsErr(err) {
                return err
            }
        }
        return nil
    })
}

