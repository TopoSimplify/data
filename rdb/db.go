package rdb

import (
	"simplex/struct/rtree"
	"simplex/geom/mbr"
	"simplex/geom"
	"bufio"
	"strings"
	"io"
)


//in-memory rtree
func NewConstDB() *rtree.RTree {
	return rtree.NewRTree(16)
}

func LoadConstDBFromGeometries(db *rtree.RTree, geoms []geom.Geometry) *rtree.RTree {
	var gs = make([]rtree.BoxObj, 0)
	for _, g := range geoms {
		gs = append(gs, g)
	}
	return db.Load(gs)
}

func LoadConstDBFromFile(db *rtree.RTree, r io.Reader) *rtree.RTree {
	objs := make([]rtree.BoxObj, 0)
	var fn = ReadBufferByLine(bufio.NewReader(r))      // line defined once
	for wkt, ok := fn(); ok; wkt, ok = fn() {
		if wkt != "" {
			objs = append(objs, geom.ReadGeometry(wkt))
		}
	}
	return db.Load(objs)
}



func SearchDb(db *rtree.RTree, query *mbr.MBR) []geom.Geometry {
	nodes := db.Search(query)
	geoms := make([]geom.Geometry, len(nodes))
	for i := range nodes {
		geoms[i] = nodes[i].GetItem().(geom.Geometry)
	}
	return geoms
}

func ReadBufferByLine(rd *bufio.Reader) (func() (string, bool)) {
	var lnrd = func() (string, bool) {
		line, err := rd.ReadString('\n')
		return strings.TrimSpace(line), err != io.EOF
	}
	return lnrd
}