package main

import (
	"gopkg.in/cheggaaa/pb.v1"
	"sort"
	"os"
	"log"
	. "simplex/geom"
	. "./store"
	. "./recon"
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
		var trajectories = SplitTraj(pings)

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

