package config

import (
	"log"
	"io/ioutil"
	"launchpad.net/goyaml"
	"os"
	"sync"
)

var config map[string]interface{}
var lock sync.RWMutex
var loaded bool

func ensureLoaded() {
	if !loaded {
		loadConfig()
	}
}

func loadConfig() {
	config = make(map[string]interface{})
	config_file := os.Getenv("PILOSA_CONFIG")
	if config_file == "" {
		config_file = "pilosa.yaml"
	}
	data, err := ioutil.ReadFile(config_file)
	if err != nil {
		log.Fatal("Problem with config file. ", err)
	}
	goyaml.Unmarshal(data, &config)
}

func GetSafe(key string) (interface{}, bool) {
	lock.RLock()
	ensureLoaded()
	defer lock.RUnlock()
	value, ok := config[key]
	return value, ok
}

func Get(key string) interface{} {
	lock.RLock()
	ensureLoaded()
	defer lock.RUnlock()
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
