package storage

import (
    "github.com/boltdb/bolt"
    "log"
    "fmt"
    "bytes"
)

const ImageDataKey = "imageData"

const JobPrefix = "job_"
const TaskPrefix = "task_"
const ImgPrefix = "img_"
const GCSPrefix = "gcs_"
const ServerPrefix = "server_"
const FinishedPrefix = "done_"

const Done = "true"
const NotDone = "false"

type Store struct {
    db *bolt.DB
}

func NewStorage(db *bolt.DB) *Store {
    if db == nil {
        log.Fatalln("storage db is not defined")
    }
    return &Store{db:db}
}



func (self *Store) MarkDone(b *bolt.Bucket) error {
    return b.Put([]byte("done"), []byte(Done))
}

func (self *Store) MarkNotDone(b *bolt.Bucket) error {
    return b.Put([]byte("done"), []byte(NotDone))
}

func (self *Store) IsDone(b *bolt.Bucket) bool {
    state := b.Get([]byte("done"))
    return string(state) == Done
}

func (self *Store) B(s string) []byte {
    return []byte(s)
}

func (self *Store) createTaskID(id interface{}) string {
    return fmt.Sprintf("%v%v", TaskPrefix, id)
}

func (self *Store) Put(b *bolt.Bucket, key string, value string) error {
    return b.Put(self.B(key), self.B(value))
}

func (self *Store) IsErr(err error) bool {
    return err != nil
}


//returns all job buckets
func (self *Store) AllJobs() []string {
    keys := make([]string, 0)
    err := self.db.View(func(tx *bolt.Tx) error {
        c := tx.Cursor()
        prefix := []byte(JobPrefix)
        for k, v := c.Last(); k != nil; k, v = c.Prev() {
            if v == nil && bytes.HasPrefix(k, prefix) {
                //only buckets
                keys = append(keys, string(k))
            }
        }
        return nil
    })
    if err != nil {
        log.Fatalln(err)
    }
    return keys

}

//returns latest bucket not done
func (self *Store) LatestJob() string {
    var key string
    err := self.db.View(func(tx *bolt.Tx) error {
        c := tx.Cursor()
        prefix := []byte(JobPrefix)
        for k, v := c.Last(); k != nil; k, v = c.Prev() {
            if v == nil && bytes.HasPrefix(k, prefix) {
                b := tx.Bucket(k)
                if b != nil && !self.IsDone(b) {
                    key = string(k)
                    break
                }
            }
        }
        return nil
    })
    if err != nil {
        log.Fatalln(err)
    }
    return key
}

//returns all job buckets
func (self *Store) DeleteBucket(bucket string) bool {
    key := []byte(bucket)
    err := self.db.Update(func(tx *bolt.Tx) error {
        if tx.Bucket(key) != nil {
            return tx.DeleteBucket(key)
        }
        return nil
    })
    if err != nil {
        log.Fatalln(err)
    }
    return true
}

//concels job by marking it as done
func (self *Store) CancelJob(bucket string) bool {
    key := []byte(bucket)
    err := self.db.Update(func(tx *bolt.Tx) error {
        b := tx.Bucket(key)
        if b != nil {
            return self.MarkDone(b)
        }
        return nil
    })
    if err != nil {
        log.Fatalln(err)
    }
    return true
}




