package config

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"launchpad.net/goyaml"

)

type Config struct {
	config   map[string]interface{}
	lock     sync.RWMutex
	filename string
	loaded   bool
}

var config *Config

func init() {
	config = NewConfig("")
}

func GetSafe(key string) (interface{}, bool) {
	return config.GetSafe(key)
}

func Get(key string) interface{} {
	return config.Get(key)
}

func GetInt(key string) int {
	return config.GetInt(key)
}

func GetString(key string) string {
	return config.GetString(key)
}

func GetStringArray(key string) []string {
	res,_:= config.GetStringArray(key)
	return res
}
func GetStringArrayDefault(key string,def []string) []string {
	res,ok := config.GetStringArray(key)

	if ! ok{
		return def
	}
	return res
}

func GetStringDefault(key string, default_value string) string {
	return config.GetStringDefault(key, default_value)
}

func GetIntDefault(key string, default_value int) int {
	return config.GetIntDefault(key, default_value)
}


func NewConfig(filename string) *Config {
	self := Config{}
	self.config = make(map[string]interface{})
	self.filename = filename
	return &self
}

func (self *Config) load() error {
	self.lock.Lock()
	defer self.lock.Unlock()
	if self.loaded {
		return nil
	}
	config_file := self.filename
	if config_file == "" {
		config_file = os.Getenv("PILOSA_CONFIG")
		if config_file == "" {
			log.Println("PILOSA_CONFIG not set, defaulting to pilosa.yaml")
			config_file = "pilosa.yaml"
		}
	}
	data, err := ioutil.ReadFile(config_file)
	if err != nil {
	
		return errors.New("Problem with config file: " + err.Error())
	}
	
	err=goyaml.Unmarshal(data, self.config)
	if err != nil{
	println(err.Error())

	}
	self.loaded = true
	return nil
}

func (self *Config) GetSafe(key string) (interface{}, bool) {
	self.load()
	self.lock.RLock()
	defer self.lock.RUnlock()
	value, ok := self.config[key]
	return value, ok
}

func (self *Config) Get(key string) interface{} {
	self.load()
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.config[key]
}

func (self *Config) GetInt(key string) int {
	value, ok := self.GetSafe(key)
	if ok {
		value_int, ok := value.(int)
		if ok {
			return value_int
		}
	}
	return 0
}
func (self *Config) GetIntDefault(key string, default_value int) int {
	value, ok := self.GetSafe(key)
	if ok {
		value_int, ok := value.(int)
		if ok {
			return value_int
		}
	}
	return default_value
}


func (self *Config) GetStringDefault(key string, default_value string) string {
	value, ok := self.GetSafe(key)
	if ok {
		value_string, ok := value.(string)
		if ok {
			return value_string
		}
	}
	return default_value
}


func (self *Config) GetString(key string) string {
	value, ok := self.GetSafe(key)
	if ok {
		value_string, ok := value.(string)
		if ok {
			return value_string
		}
	}
	return ""
}



func (self *Config) GetStringArray(key string) ([]string,bool){
	value, ok := self.GetSafe(key)
	
	if ok {
		var results []string
		for _,v:= range value.([]interface{}){
			results=append(results,v.(string))
		}
			return results,ok
	}
	return []string{},ok
}

