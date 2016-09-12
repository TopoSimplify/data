package main

import (
    "gopkg.in/cheggaaa/pb.v1"
    "sort"
    "os"
    "log"
    . "simplex/geom"
    . "./store"
)

const DBPath = "/home/titus/01/dev/godev/src/simplex/data/db/mtraffic.db"
const MtrajPath = "/home/titus/01/dev/godev/src/simplex/data/db/mtraj.db"

func main() {
    var mtStore = NewStorage(DBPath)
    defer mtStore.Close()
    var mtrjStore = NewStorage(MtrajPath)
    defer mtrjStore.Close()

    var vessels = mtStore.AllVessels()
    sort.Ints(vessels)

    fid, err := os.Create("/home/titus/01/dev/godev/src/simplex/data/tmp/tj.wkt")
    if err != nil {
        log.Fatalln(err)
    }
    defer fid.Close()

    bar := pb.StartNew(len(vessels))
    for _, v := range vessels {
        bar.Increment()
        var pings = mtStore.AllPings(v)
        trajectories := traj_construct(pings)

        for _, trjs := range trajectories {
            coords := make([]*Point, 0)
            for _, tj := range trjs {
                coords = append(coords, NewPointXY(tj.X, tj.Y))
            }
            fid.WriteString(NewLineString(coords).WKT() + "\n")
        }
    }
    bar.FinishPrint("done!")
}

func traj_construct(pings []*MTraffic) [][]*MTraffic {
    trajectories := make([][]*MTraffic, 0)
    traj := make([]*MTraffic, 0)

    var fn_flush = func() {
        if len(traj) > 25 {
            trajectories = append(trajectories, traj)
        }
        traj = make([]*MTraffic, 0)
    }

    for i := range pings {
        if i == 0 {
            traj = append(traj, pings[i])
        } else {
            n := len(traj)
            a, b := traj[n - 1], pings[i]
            dt := time_delta(a, b)

            if (dt <= 1) || (dt > 1 && dt <= 2) {
                traj = append(traj, b)
            } else {
                fn_flush()
                traj = append(traj, b)
            }
        }
    }

    if len(traj) > 0 {
        fn_flush()
    }

    return trajectories
}

func time_delta(a, b *MTraffic) float64 {
    dt := b.Time.Sub(a.Time)
    return dt.Hours()
}
