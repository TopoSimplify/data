package main

import (
	"log"
	"simplex/data/store"
	"simplex/data/config"
	"github.com/intdxdt/math"
	"gopkg.in/cheggaaa/pb.v1"
	"github.com/intdxdt/geom"
	"github.com/intdxdt/rtree"
)

const (
	Join = iota
	Split
	Drop
)

var conf config.Config
func init () {
	conf = config.ReadConfig("mconfig.toml")
}

func main() {
	var mtDB = store.NewStorage(conf.DBPath)
	defer mtDB.Close()
	//-----------------------------------------'
	var tjDB = store.NewStorage(conf.MtrajPath)
	defer tjDB.Close()
	//-----------------------------------------'

	var vessels = mtDB.AllVessels()
	var db = store.LoadFromShpFile(store.NewDB(), conf.ShpData)
	ProcessVessels(mtDB, tjDB, vessels, db)
}

func ProcessVessels(mtDB, tjDB *store.Store, vessels [][]byte, db *rtree.RTree) {

	bar := pb.StartNew(len(vessels))
	var trajectories = make([]*store.MTraj, 0)

	for _, key := range vessels {
		bar.Increment()
		var pings = mtDB.AllPings(key)
		var tokens = store.SplitTraj(pings)
		var components = ComposeTrajs(tokens, db)
		for _, comp := range components {
			trajs := make([]*store.MTraffic, 0)
			for _, obj := range comp {
				trajs = append(trajs, obj.Mt())
			}
			if len(trajs) > 1 {
				CheckTiming(trajs)
				mmsi := trajs[0].MMSI
				tj := &store.MTraj{MMSI:mmsi, Traj:trajs}
				trajectories = append(trajectories, tj)
			}
		}

		//save trajectories to disk
		//----------------------------------------------
		if len(trajectories) >= conf.TrajBufferLimit {
			tjDB.BulkLoadTrajStorage(trajectories)
			trajectories = make([]*store.MTraj, 0)
		}
		//----------------------------------------------
	}
	bar.FinishPrint("done!")
}

func CheckTiming(data []*store.MTraffic){
	for i := 0 ; i < len(data) -1 ; i++ {
		bln := data[i].Time.Equal(data[i+1].Time) ||data[i].Time.Before(data[i+1].Time)
		if !bln {
			log.Fatalln("inconsistent timing...")
		}
	}
}
func ComposeTrajs(trajectories [][]*store.Obj, db *rtree.RTree) [][]*store.Obj {
	var comp = make([][]*store.Obj, 0)

	for len(trajectories) > 0 {
		var n = len(trajectories)
		if len(trajectories) == 0 {
			break
		}

		var _a, _b, _ab []*store.Obj
		var a, b, c *store.Obj

		a = store.Last(trajectories[0])
		if n > 1 {
			b = store.First(trajectories[1])
		}

		if n == 3 {
			c = store.First(trajectories[2])
		}
		var first, last []*store.Obj
		if len(trajectories) > 0 {
			first = trajectories[0]
		}
		if len(trajectories) > 1 {
			last = trajectories[1]
		}

		var state = Inter(first, last, []*store.Obj{a, b}, c, db)

		if state == Drop {
			store.Shift(&trajectories)
			store.Shift(&trajectories)
			continue
		} else if state == Split {
			_a = store.Shift(&trajectories)
			if len(_a) > 1 {
				comp = append(comp, _a)
			}
			continue
		} else if state == Join {
			_a = store.Shift(&trajectories)
			_b = store.Shift(&trajectories)
			_ab = store.Extend(_a, _b)
			trajectories = store.UnShiftQueue(&trajectories, _ab)
		}
	}
	return comp
}

func Inter(first, last, data []*store.Obj, c *store.Obj, db *rtree.RTree) int {

	var ln *geom.LineString
	var pnts = make([]*geom.Point, 0)
	var geomlist = make([]geom.Geometry, 0)

	for _, m := range data {
		if m != nil {
			mt := m.Mt()
			pt := geom.NewPointXY(mt.X, mt.Y)
			pnts = append(pnts, pt)
			geomlist = append(geomlist, pt)
		}
	}

	var pta, ptb = pnts[0], pnts[0]
	if len(pnts) > 1 {
		ptb = pnts[1]
	}
	if len(pnts) == 1 && len(geomlist) == 1 {
		geomlist = append(geomlist, pnts[0])
	}

	var ab_coincides = pta.Equals2D(ptb)

	if ab_coincides {
		pnts = append(pnts, pta)
	} else {
		ln = geom.NewLineString(pnts)
		geomlist = append(geomlist, ln)
	}

	var inter_a = Intersects(db, geomlist[0])
	var inter_b = Intersects(db, geomlist[1])
	var inter_a_b = inter_a

	if !ab_coincides {
		inter_a_b = Intersects(db, geomlist[2])
	}
	return caseinter(first, last, data, c, []bool{inter_a, inter_b, inter_a_b })
}

func caseinter(first, last, args []*store.Obj, c *store.Obj, results []bool) int {
	var aint = results[0]
	var bint = results[1]
	var a_b_int = results[2]

	var a = args[0]
	var b = args[1]

	if isNill(a) || isNill(b) {
		return Split
	}

	var gapoverflow = func() bool {
		if !isNill(b) {
			return b.Dt() > 18.0
		}
		return false
	}

	var nullcase = isZero(a.Dt()) && isZero(a.Ddt()) && isZero(b.Ddt()) && b.Dt() < 1

	var case1 = aint && bint && b.Dt() < 1 && b.Ddt() < 1
	var case2 = (F(aint) || F(bint)) && (aint != bint) && b.Dt() < 1 && b.Ddt() < 1

	var case3 = F(a_b_int) && F(case1) && F(case2)  && b.Dt() <= 1
	var case3gap = F(a_b_int) && F(case1) && F(case2) && b.Dt() > 1 && b.Dt() <= 18

	var case4 = len(first) == 1 && !isNill(b)
	var case4t = case4 && b.Dt() < 1
	var case4inter = case4 && (a_b_int || gapoverflow())

	var case5 = !isNill(c) && !isZero(b.Dt()) && !isZero(c.Dt()) && (b.Dt() < c.Dt())
	var case5t = case5 && b.Dt() < 1
	var case5inter = case5 && (a_b_int || gapoverflow())

	var case6 = len(last) == 1 && !isNill(c) && !case5

	var csee7inter = len(last) == 1 && a_b_int

	if nullcase || case4t || case5t {
		return Join
	} else if case4inter {
		return Drop
	} else if case5inter {
		if len(first) > 0 {
			return Split
		}
		return Drop
	} else if case1 || case2 || case4 || case5 {
		return Join
	} else if case3 || case3gap {
		return Join
	} else if case6 || csee7inter {
		if len(first) > 0 {
			return Split
		}
		return Drop
	} else {
		if len(first) > 1 {
			return Split
		}
	}
	return Drop
}

func F(b bool) bool {
	return !b
}
func T(b bool) bool {
	return b
}

func Intersects(db *rtree.RTree, geom geom.Geometry) bool {
	var bln = false
	if db != nil {
		results := store.SearchDb(db, geom.BBox())
		for i := 0; !bln && i < len(results); i++ {
			bln = geom.Intersects(results[i])
		}

	}
	return bln
}

func isNill(o *store.Obj) bool {
	return o == nil
}

func isZero(v float64) bool {
	return math.FloatEqual(v, 0.0)
}