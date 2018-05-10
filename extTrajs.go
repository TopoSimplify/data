package main

import (
	"log"
	"os"
	"encoding/json"
	"github.com/boltdb/bolt"
	"github.com/intdxdt/geom"
	"github.com/TopoSimplify/data/store"
	"github.com/TopoSimplify/data/config"
)

const TrajPtSize = 200

var conf config.Config

func init() {
	conf = config.ReadConfig("mconfig.toml")
}

func main() {
	pth := "/home/titus/01/data/mtraj.db"
	s := store.NewStorage(pth)
	defer s.Close()

	var limitSize = 200

	keys := s.TrajKeys()
	trajectories := make([]*geom.LineString, 0)
loop:
	for _, key := range keys {
		trjs := fetchTrajectories(s, key)
		for _, tj := range trjs {
			if len(trajectories) == limitSize {
				break loop
			}
			trajectories = append(trajectories, tj)
		}
	}

	fid, err := os.Create(conf.WKT)
	defer fid.Close()

	if err != nil {
		log.Fatalln(err)
	}

	for _, o := range trajectories {
		fid.WriteString(o.WKT() + "\n")
	}
}

func fetchTrajectories(s *store.Store, key []byte) []*geom.LineString {
	trajectories := make([]*geom.LineString, 0)
	err := s.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		c := tx.Bucket(key).Cursor()

		for k, v := c.First(); len(k) != 0; k, v = c.Next() {

			var mtrj store.MTraj
			err := json.Unmarshal(v, &mtrj)
			if err != nil {
				log.Fatalln(err)
			}

			trj := make([]*geom.Point, 0)
			for _, tj := range mtrj.Traj {
				trj = append(trj, geom.NewPointXYZ(tj.X, tj.Y, float64(tj.Time.Unix())))
			}
			if len(trj) > TrajPtSize {
				trajectories = append(trajectories, geom.NewLineString(trj))
			}

		}
		return nil
	})

	if err != nil {
		log.Fatalln(err)
	}

	return trajectories
}
