package main

import (
	"log"
	"fmt"
	"bytes"
	"encoding/gob"
	"encoding/base64"
	"github.com/TopoSimplify/data/store"
)

// go binary encoder
func Serialize(n *store.MTraj) string {
	var buf bytes.Buffer
	var err = gob.NewEncoder(&buf).Encode(n)
	if err != nil {
		log.Fatalln(err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

// go binary decoder
func Deserialize(str string) *store.MTraj {
	var n *store.MTraj
	var dat, err = base64.StdEncoding.DecodeString(str)
	if err != nil {
		log.Fatalln(`failed base64 Decode`, err)
	}
	var buf bytes.Buffer
	_, err = buf.Write(dat)
	if err != nil {
		log.Fatalln(`failed to write to buffer`)
	}
	err = gob.NewDecoder(&buf).Decode(&n)
	if err != nil {
		fmt.Println(`failed gob Decode`, err)
	}
	return n
}
