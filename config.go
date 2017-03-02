package main

import (
	"log"
	"reflect"
	"github.com/pelletier/go-toml"
	"gopkg.in/oleiade/reflections.v1"
	"fmt"
)


type Config struct {
	CSVPath         string   `json:"csvpath"`
	DBPath          string   `json:"dbpath"`
	MtrajPath       string   `json:"mtrajpath"`
	ShpData         string   `json:"shpdata"`
	WKT             string   `json:"wkt"`
	CSVBufferLimit int   `json:"csvbuflimit"`
	TrajBufferLimit int   `json:"trajbuflimit"`
}


var conf Config

func init () {
	conf = readConfig("mconfig.toml")
}

//parse config file
func readConfig(fname string) Config{
	conf := Config{}

	config, err := toml.LoadFile(fname)
	if err != nil {
		log.Fatal(err)
	}

	fields, err := reflections.Fields(conf)
	confmap := config.ToMap()

	for _, f := range fields {
		var fv interface{}

		if k, _ := reflections.GetFieldKind(&conf, f); k == reflect.String {
			fv = confmap[f].(string)
		} else if k, _ := reflections.GetFieldKind(&conf, f); k == reflect.Int {
			v := confmap[f].(int64)
			fv  = int(v)
		}
		reflections.SetField(&conf, f, fv)
	}

	return conf
}
