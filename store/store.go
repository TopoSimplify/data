package store

import (
    "github.com/boltdb/bolt"
    "log"
    "strconv"
    "encoding/json"
)

type Store struct {
    db *bolt.DB
}

func NewStorage(filename string) *Store {
    return &Store{db:open_store(filename)}
}

func (store *Store) Close() {
    if store.db != nil {
        store.db.Close()
    }
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
    err := self.db.View(func(tx *bolt.Tx) error {
        // Assume bucket exists and has keys
        tx.ForEach(func(k []byte, _ *bolt.Bucket) error {
            id, err := strconv.Atoi(string(k))
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
//returns all job buckets
func (self *Store) AllPings(id int) []*MTraffic {
    var traffic = make([]*MTraffic, 0)
    err := self.db.View(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte(strconv.Itoa(id)))
        c := b.Cursor()
        for k, v := c.First(); k != nil; k, v = c.Next() {
            if v != nil {
                var mt = &MTraffic{}
                if err := json.Unmarshal(v, mt); err != nil {
                    return err
                }
                traffic = append(traffic, mt)
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
func open_store(filename string) *bolt.DB {
    db, err := bolt.Open(filename, 0600, nil)
    if err != nil {
        log.Fatal(err)
    }
    return db
}



