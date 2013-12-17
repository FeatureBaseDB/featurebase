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
	config map[string]interface{}
	lock   sync.RWMutex
	loaded bool
}

var config *Config

func init() {
	config_file := os.Getenv("PILOSA_CONFIG")
	if config_file == "" {
		config_file = "pilosa.yaml"
	}
	var err error
	config, err = NewConfig(config_file)
	if err != nil {
		log.Fatal(err)
	}
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

func NewConfig(filename string) (*Config, error) {
	self := Config{}
	self.config = make(map[string]interface{})
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, errors.New("Problem with config file: " + err.Error())
	}
	goyaml.Unmarshal(data, self.config)
	return &self, nil
}

func (self *Config) GetSafe(key string) (interface{}, bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	value, ok := self.config[key]
	return value, ok
}

func (self *Config) Get(key string) interface{} {
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
