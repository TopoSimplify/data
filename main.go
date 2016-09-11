package main

import (
	"github.com/boltdb/bolt"
	"log"
	"fmt"
	"os"
	"encoding/csv"
	"bufio"
	"io"
	"bytes"
	"strconv"
	"time"
	"strings"
	"simplex/prj"
	. "./store"
)

const UploadDBPath = "db/mtraffic.db"

var mtDB *bolt.DB
var BufferLimit = 1000

var proj = prj.NewSRS(4326).AsGeographic().To(prj.NewSRS(3857))

func main() {
	OpenStorage()
	var mtStore = NewStorage(mtDB)
	load_file("tmp/data/AUG13.csv", mtStore)

	defer CloseStorage()
	fmt.Println("done !")
}

//open upload db
func OpenStorage() {
	db, err := bolt.Open(UploadDBPath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	mtDB = db
}

//close upoad storage
func CloseStorage() {
	if mtDB != nil {
		mtDB.Close()
	}
}

//load file
func load_file(fname string, mtStore *Store) {
	fid, err := os.Open(fname)
	if err != nil {
		log.Fatalln(err)
	}
	r := csv.NewReader(bufio.NewReader(fid))

	//mt buffer
	var mtbuffer = make([]*MTraffic, 0)

	//empty buffer
	var drainBuffer = func() {
		err := mtStore.BulkLoadStorage(mtbuffer)
		if err != nil {
			log.Fatalln(err)
		}
		//empty the buffer
		mtbuffer = make([]*MTraffic, 0)
	}

	for {
		record, err := r.Read()
		if err == io.EOF {
			if len(mtbuffer) != 0 {
				drainBuffer();
			}
		}
		if err != nil {
			log.Fatal(err)
		}

		mt, err := mtraffic_record(record)
		if err != nil {
			log.Fatalln(err)
		} else {
			//add item to buffer
			mtbuffer = append(mtbuffer, mt)
			//@limit
			if len(mtbuffer) == BufferLimit {
				drainBuffer()
			}
		}

	}
}

func mtraffic_record(line []string) (*MTraffic, error) {
	var iN = 3
	var initvals = make([]int, iN)
	for i, v := range line[:iN] {
		val, err := parseInt(v)
		if err != nil {
			return nil, err
		}
		initvals[i] = val
	}
	imonum := initvals[0]
	mmsi := initvals[1]
	status := initvals[2]

	station := line[3]
	speed, err := parseFloat(line[4])
	if err != nil {
		return nil, err
	}

	lng, err := parseFloat(line[5])
	if err != nil {
		return nil, err
	}

	lat, err := parseFloat(line[6])
	if err != nil {
		return nil, err
	}

	course, err := parseFloat(line[7])
	if err != nil {
		return nil, err
	}

	heading, err := parseFloat(line[8])
	if err != nil {
		return nil, err
	}

	timestamp, err := parseTime(line[9])
	if err != nil {
		return nil, err
	}

	vesseltype, err := parseInt(line[10])
	if err != nil {
		return nil, err
	}

	x, y, err := proj.Trans(lng, lat)
	if err != nil {
		return nil, err
	}

	return &MTraffic{
		IMOnum  : imonum,
		MMSI    : mmsi,
		Status  : status,
		Station : station,
		Speed   : speed,
		X     	: x,
		Y     	: y,
		Course  : course,
		Heading : heading,
		Time    : timestamp,
		Type    : vesseltype,
	}, nil
}

func parseInt(v string) (int, error) {
	var vv = bytes.Trim([]byte(v), "\xef\xbb\xbf")
	return strconv.Atoi(string(vv))
}

func parseFloat(v string) (float64, error) {
	var vv = bytes.Trim([]byte(v), "\xef\xbb\xbf")
	return strconv.ParseFloat(string(vv), 64)
}

func parseTime(v string) (time.Time, error) {
	if !strings.Contains(v, "T") {
		tokens := strings.Split(v, " ")
		v = strings.Join(tokens, "T")
	}
	if !strings.Contains(v, "Z") {
		v = v + "Z"
	}
	vv, _ := time.Parse(time.RFC3339, v)
	return vv, nil
}




