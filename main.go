package main

import (
    "github.com/boltdb/bolt"
    "log"
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
    "path/filepath"
    "gopkg.in/cheggaaa/pb.v1"
    "github.com/tj/go-spin"
    "path"
    "fmt"
)

const UploadDBPath = "/home/titus/01/dev/godev/src/simplex/data/db/mtraffic.db"

var mtDB *bolt.DB
var TotalLoad = 0
var BufferLimit int = 100000
var CurFile string

var proj = prj.NewSRS(4326).AsGeographic().To(prj.NewSRS(3857))

func main() {
    OpenStorage()
    defer CloseStorage()

    var mtStore = NewStorage(mtDB)
    var mmsi_files = fetchFiles("/home/titus/01/dev/godev/src/simplex/data/tmp/*.csv")
    bar := pb.StartNew(len(mmsi_files))
    for _, file := range mmsi_files {
        bar.Increment()
        fmt.Print("\n\n")
        _, CurFile = path.Split(file)
        bulk_load_mtraffic(file, mtStore)
    }
    bar.FinishPrint("done!")
    fmt.Printf("\nBulkloaded %v points\n", TotalLoad)
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

//fetch files in directory
func fetchFiles(src_pattern string) []string {
    files, err := filepath.Glob(src_pattern)
    if err != nil {
        log.Fatalln(err)
    }
    return files
}


//load file
func bulk_load_mtraffic(fname string, mtStore *Store) {
    defer func() {
        if r := recover(); r != nil {
            fmt.Println("Recovered bulk load", r)
        }
    }()

    spn := spin.New()
    fid, err := os.Open(fname)
    defer fid.Close()

    if err != nil {
        log.Println("file :" + CurFile + "could not be opened")
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
        TotalLoad += len(mtbuffer)
        //empty the buffer
        mtbuffer = make([]*MTraffic, 0)
        fmt.Printf("\r  \033[36mbulk:%v | loaded:%v\033[m %s ", CurFile, TotalLoad, spn.Next())
    }

    for {
        record, err := r.Read()
        if err == io.EOF {
            if len(mtbuffer) != 0 {
                drainBuffer();
            }
            break
        }
        if err != nil {
            log.Println("unable to read line")
            continue
        }

        mt, err := mtraffic_record(record)
        if err != nil {
            print(err)
            print("\n")
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
        X        : x,
        Y        : y,
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




