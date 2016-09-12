package store

import (
	"fmt"
	"github.com/boltdb/bolt"
	"strconv"
)
//string to byte
func B(s string) []byte {
	return []byte(s)
}

//int to byte
func ItoB(i int) []byte {
	return B(strconv.Itoa(i))
}

//create id
func createTaskID(id interface{}) string {
	return fmt.Sprintf("%v", id)
}

//put string
func PutS(b *bolt.Bucket, key string, value string) error {
	return Put(b, B(key), B(value))
}
//put
func Put(b *bolt.Bucket, key []byte, value []byte) error {
	return b.Put(key, value)
}

//is error
func IsErr(err error) bool {
	return err != nil
}
