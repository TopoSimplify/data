package main

import (
	"log"
	"fmt"
	"bytes"
	"text/template"
	"simplex/db"
)

const IdColumn = "id"
const GeomColumn = "geom"

var onlineTblTemplate = `
CREATE TABLE IF NOT EXISTS {{.Table}} (
    id          SERIAL NOT NULL,
    node        TEXT NOT NULL,
    size        INT NOT NULL,
    length      real NOT NULL
) WITH (OIDS=FALSE);
CREATE INDEX idx_size_{{.Table}} ON {{.Table}} (size);
CREATE INDEX idx_length_{{.Table}} ON {{.Table}} (length);
`

var onlineTemplate *template.Template

func init() {
	var err error
	onlineTemplate, err = template.New("online_table").Parse(onlineTblTemplate)
	if err != nil {
		log.Fatalln(err)
	}
}

func CreateNodeTable(Src *db.DataSrc) error {
	var query bytes.Buffer
	if err := onlineTemplate.Execute(&query, Src); err != nil {
		log.Fatalln(err)
	}
	fmt.Println(query.String())
	var tblSQl = fmt.Sprintf(`DROP TABLE IF EXISTS %v CASCADE;`, Src.Table)
	if _, err := Src.Exec(tblSQl); err != nil {
		log.Panic(err)
	}
	_, err := Src.Exec(query.String())
	return err
}
