package rdb

import (
	"github.com/jonas-p/go-shp"
	"simplex/struct/rtree"
	"simplex/geom"
	"log"
)

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
