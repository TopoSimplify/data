package store

import (
	"github.com/intdxdt/rtree"
	"github.com/jonas-p/go-shp"
	"log"
	"github.com/intdxdt/geom"
	"bufio"
	"strings"
	"io"
	"github.com/intdxdt/mbr"
)



//in-memory rtree
func NewDB() *rtree.RTree {
	return rtree.NewRTree(16)
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


func LoadFromShpFile(db *rtree.RTree, file_name string) *rtree.RTree {
	// open a shapefile for reading
	shape, err := shp.Open(file_name)
	if err != nil {
		log.Fatal(err)
	}
	defer shape.Close()

	//gs := make([]geom.Geometry, 0)
	objs := make([]rtree.BoxObj, 0)
	// loop through all features in the shapefile
	for shape.Next() {
		_, polygon := shape.Shape()
		ply := polygon.(*shp.Polygon)

		coords := make([]*geom.Point, 0)
		for _, pt := range ply.Points {
			coords = append(coords, geom.NewPointXY(pt.X, pt.Y))
		}
		objs = append(objs, geom.NewPolygon(coords))
	}
	return db.Load(objs)
}

