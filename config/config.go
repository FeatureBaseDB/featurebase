package config

import (
	"flag"
	"log"
	"io/ioutil"
	"launchpad.net/goyaml"
)

var config map[string]interface{}

func GetSafe(key string) (interface{}, bool) {
	value, ok := config[key]
	return value, ok
}

func Get(key string) interface{} {
	return config[key]
}

func GetInt(key string) int {
	value, ok := GetSafe(key)
	if ok {
		value_int, ok := value.(int)
		if ok {
			return value_int
		}
	}
	return 0
}

func GetString(key string) string {
	value, ok := GetSafe(key)
	if ok {
		value_string, ok := value.(string)
		if ok {
			return value_string
		}
	}
	return ""
}

func init() {
	log.Println("Start config")
	config = make(map[string]interface{})
	config_file := flag.String("config", "pilosa.yaml", "Path to config file.")
	flag.Parse()
	data, err := ioutil.ReadFile(*config_file)
	if err != nil {
		log.Fatal("Problem with config file. ", err)
	}
	goyaml.Unmarshal(data, &config)
}
