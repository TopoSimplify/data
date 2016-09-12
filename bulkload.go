package main

import (
    "log"
    "os"
    "encoding/csv"
    "bufio"
    "io"
    "simplex/prj"
    "gopkg.in/cheggaaa/pb.v1"
    "github.com/tj/go-spin"
    "path"
    "fmt"
    . "./store"
)

const DBPath = "/home/titus/01/dev/godev/src/simplex/data/db/mtraffic.db"

var TotalLoad = 0
var BufferLimit int = 100000
var CurFile string

var proj = prj.NewSRS(4326).AsGeographic().To(prj.NewSRS(3857))

func main() {
    var mtStore = NewStorage(DBPath)
    defer mtStore.Close()

    var fpath = "/home/titus/01/dev/godev/src/simplex/data/tmp/data/*.csv"
    var mmsi_files = FetchFiles(fpath)

    bar := pb.StartNew(len(mmsi_files))
    for _, file := range mmsi_files {
        bar.Increment()
        fmt.Println()
        _, CurFile = path.Split(file)
        bulk_load_mtraffic(file, mtStore)
    }

    bar.FinishPrint("done!")
    fmt.Printf("\nBulkloaded %v points\n", TotalLoad)
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
        val, err := ParseInt(v)
        if err != nil {
            return nil, err
        }
        initvals[i] = val
    }
    imonum := initvals[0]
    mmsi := initvals[1]
    status := initvals[2]

    station := line[3]
    speed, err := ParseFloat(line[4])
    if err != nil {
        return nil, err
    }

    lng, err := ParseFloat(line[5])
    if err != nil {
        return nil, err
    }

    lat, err := ParseFloat(line[6])
    if err != nil {
        return nil, err
    }

    course, err := ParseFloat(line[7])
    if err != nil {
        return nil, err
    }

    heading, err := ParseFloat(line[8])
    if err != nil {
        return nil, err
    }

    timestamp, err := ParseTime(line[9])
    if err != nil {
        return nil, err
    }

    vesseltype, err := ParseInt(line[10])
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





