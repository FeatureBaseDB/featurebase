package core

import (
	"net/http"
	"encoding/json"
)

func (service *Service) ServeHTTP() {
	http.HandleFunc("/message", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}
		var message Message
		decoder := json.NewDecoder(r.Body)
		if decoder.Decode(&message) != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		service.Inbox <- &message
	})
	http.ListenAndServe(string(service.HttpLocation.Port), nil)
}
