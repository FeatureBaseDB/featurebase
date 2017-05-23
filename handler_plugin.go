package pilosa

import (
	"fmt"
	"sync"

	"github.com/gorilla/mux"
)

type HandlerPluginConstructor func(*Handler, *mux.Router)

type handlerPluginRegistry struct {
	mutex   *sync.RWMutex
	plugins map[string]HandlerPluginConstructor
}

var handlerPlugins = &handlerPluginRegistry{
	mutex:   &sync.RWMutex{},
	plugins: map[string]HandlerPluginConstructor{},
}

func RegisterHandlerPlugin(prefix string, c HandlerPluginConstructor) error {
	handlerPlugins.mutex.Lock()
	defer handlerPlugins.mutex.Unlock()
	if _, found := handlerPlugins.plugins[prefix]; found {
		return fmt.Errorf("%s handler plugin was already registered", prefix)
	}
	handlerPlugins.plugins[prefix] = c
	return nil
}

func attachHandlerPlugins(handler *Handler, router *mux.Router) {
	handlerPlugins.mutex.Lock()
	defer handlerPlugins.mutex.Unlock()
	for prefix, constructor := range handlerPlugins.plugins {
		subrouter := router.PathPrefix("/" + prefix).Subrouter()
		constructor(handler, subrouter)
	}

}
