package config

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"github.com/nu7hatch/gouuid"
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
	goyaml.Unmarshal(data, self.config)
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

func GetUUID(key string) *uuid.UUID {
	value, ok := GetSafe(key)
	if ok {
		value_uuid, err := uuid.ParseHex(value.(string))
		if err == nil {
			return value_uuid
		}
	}
	return nil
}
