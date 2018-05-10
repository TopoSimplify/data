package main

import (
	"log"
	"fmt"
	"bytes"
	"io/ioutil"
	"github.com/TopoSimplify/db"
	"database/sql"
	"path/filepath"
	"encoding/json"
	"github.com/naoina/toml"
	"github.com/boltdb/bolt"
	"github.com/intdxdt/geom"
	"github.com/TopoSimplify/data/store"
	"github.com/TopoSimplify/streamdp/common"
	"github.com/TopoSimplify/streamdp/config"
)

func main() {
	var pwd = common.ExecutionDir()
	var srcFile = filepath.Join(pwd, "../resource/src.toml")

	var serverCfg = (&config.ServerConfig{}).Load(srcFile)
	var dbCfg = serverCfg.DBConfig()

	var sqlsrc, err = sql.Open("postgres", fmt.Sprintf(
		"user=%s password=%s dbname=%s sslmode=disable",
		dbCfg.User, dbCfg.Password, dbCfg.Database,
	))
	if err != nil {
		log.Panic(err)
	}
	var src = &db.DataSrc{
		Src:    sqlsrc,
		Config: dbCfg,
		SRID:   serverCfg.SRID,
		Dim:    serverCfg.Dim,
		Table:  serverCfg.Table,
	}
	createAndProcessTables(src)
	//extractTrajectories(src)

	//loadTraj(src)

	fmt.Println("<done>")
}

func loadTraj(src *db.DataSrc){
	var dest = "/media/dxdt/trajdata"
	var mtraj store.MTraj
	var fname = fmt.Sprintf(`%v/%v`, dest, "101.toml")
	var dat , err = ioutil.ReadFile(fname)
	if err != nil {
		log.Panic(err)
	}
	err = toml.Unmarshal(dat, &mtraj)
	if err != nil {
		log.Panic(err)
	}
}

func extractTrajectories(src *db.DataSrc) {
	var query = fmt.Sprintf(`
			SELECT id, node, size, length
			FROM %v
			ORDER BY  size desc, length desc limit %v;
		`, src.Table, 2000,
	)
	h, err := src.Query(query)
	if err != nil {
		log.Panic(err)
	}
	var dest = "/media/dxdt/trajdata"
	for h.Next() {
		var node string
		var id, size int
		var length float64

		h.Scan(&id, &node, &size, &length)
		var mtraj = Deserialize(node)
		mtraj.MMSI = id
		dat, err := toml.Marshal(mtraj)
		if err != nil {
			log.Panic(err)
		}

		var fname = fmt.Sprintf(`%v/%v.toml`, dest, id)
		err = ioutil.WriteFile(fname, dat, 0644)
		if err != nil {
			log.Panic(err)
		}
	}
}

func createAndProcessTables(src *db.DataSrc) {
	err := CreateNodeTable(src)
	if err != nil {
		log.Fatal(err)
	}

	pth := "/home/titus/01/data/mtraj.db"
	s := store.NewStorage(pth)
	defer s.Close()

	total := 0
	keys := s.TrajKeys()
	for _, key := range keys {
		total += procTrajectories(s, key, src)
		fmt.Println("processed : ", total)
	}
}

func procTrajectories(s *store.Store, key []byte, src *db.DataSrc) int {
	total       := 0
	bufferLimit := 100
	rows        := make([]string, 0)

	fnInsert := func() {
		insertIntoTable(src, rows)
		total += len(rows)
		rows = make([]string, 0)
	}

	err := s.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		c := tx.Bucket(key).Cursor()

		for k, v := c.First(); len(k) != 0; k, v = c.Next() {
			var mtrj store.MTraj
			err := json.Unmarshal(v, &mtrj)
			if err != nil {
				log.Panic(err)
			}

			trj := make([]*geom.Point, 0)
			for _, tj := range mtrj.Traj {
				trj = append(trj, geom.NewPointXYZ(tj.X, tj.Y, float64(tj.Time.Unix())))
			}
			g := geom.NewLineString(trj)
			// g.WKT(), src.SRID
			// ST_GeomFromText('%v', %v)
			row := fmt.Sprintf(`'%v', %v, %v, ST_GeomFromText('%v', %v)`,
				Serialize(&mtrj), len(mtrj.Traj), g.Length(), g.WKT(), src.SRID )
			rows = append(rows, row)
			if len(rows) > bufferLimit {
				fnInsert()
			}
		}
		if len(rows) > 0 {
			fnInsert()
		}
		return nil
	})

	if err != nil {
		log.Fatalln(err)
	}
	return total
}

func insertIntoTable(src *db.DataSrc, rows []string) {
	var buf bytes.Buffer
	var n = len(rows) - 1
	var columns = "node, size, length, geom"
	for i, row := range rows {
		buf.WriteString("(" + row + ")")
		if i < n {
			buf.WriteString(",\n")
		}
	}
	insertSQL := fmt.Sprintf(
		"INSERT INTO  %s (%s) VALUES \n%s;",
		src.Table, columns, buf.String(),
	)
	if _, err := src.Exec(insertSQL); err != nil {
		log.Panic(err)
	}
}
