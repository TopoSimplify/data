package store

import (
	"bufio"
	"github.com/intdxdt/geom"
	"github.com/intdxdt/mbr"
	"github.com/intdxdt/rtree"
	"github.com/jonas-p/go-shp"
	"io"
	"log"
	"strings"
)

// in-memory rtree
func NewDB() *rtree.RTree {
	return rtree.NewRTree(16)
}

func SearchDb(db *rtree.RTree, query *mbr.MBR) []geom.Geometry {
	nodes := db.Search(*query)
	geoms := make([]geom.Geometry, len(nodes))
	for i := range nodes {
		geoms[i] = nodes[i].(geom.Geometry)
	}
	return geoms
}

func ReadBufferByLine(rd *bufio.Reader) func() (string, bool) {
	var lnrd = func() (string, bool) {
		line, err := rd.ReadString('\n')
		return strings.TrimSpace(line), err != io.EOF
	}
	return lnrd
}

func LoadFromShpFile(db *rtree.RTree, file_name string) *rtree.RTree {
	// open a shapefile for reading
	shape, err := shp.Open(file_name)
	if err != nil {
		log.Fatal(err)
	}
	defer shape.Close()


	var objs = make([]rtree.BoxObject, 0)
	// loop through all features in the shapefile
	for shape.Next() {
		_, polygon := shape.Shape()
		ply := polygon.(*shp.Polygon)

		coords := make([]geom.Point, 0)
		for _, pt := range ply.Points {
			coords = append(coords, geom.PointXY(pt.X, pt.Y))
		}
		objs = append(objs, geom.NewPolygon(geom.Coordinates(coords)))
	}
	return db.Load(objs)
}
