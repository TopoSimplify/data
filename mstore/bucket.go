package storage

import "github.com/boltdb/bolt"

func create_sub_bucket(b *bolt.Bucket, key []byte) (*bolt.Bucket, error) {
    return b.CreateBucketIfNotExists(key)
}
