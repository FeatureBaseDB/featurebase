package main

import (
	"encoding/hex"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt"
)

const (
	adminToken  = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiYWRtaW4ifQ.I1iCgk1VU7m6e-En4ACTHIs6V2dZpy_8j2blSSo7K3U"
	readerToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoicmVhZGVyIn0.QcHy_W6oAYFgdBWy1CqLr55HcOyymn5zAXPJUKCvQE4"
	writerToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoid3JpdGVyIn0.aEk-12xP9RJeXog4MHO8LhuQFEjNNG2BcWDcMzSX_HI"
)

func groups(w http.ResponseWriter, req *http.Request) {
	authToken := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
	status := http.StatusOK

	var val []byte
	switch authToken {
	case adminToken:
		val = []byte(`{"value":[{"id":"group-id-admin","displayName":"group-id-admin"}]}`)
	case readerToken:
		val = []byte(`{"value":[{"id":"group-id-reader","displayName":"group-id-reader"}]}`)
	case writerToken:
		val = []byte(`{"value":[{"id":"group-id-writer","displayName":"group-id-writer"}]}`)
	default:
		status = http.StatusUnauthorized
	}
	w.WriteHeader(status)
	w.Write(val)
}

func token(w http.ResponseWriter, req *http.Request) {
	tkn := jwt.New(jwt.SigningMethodHS256)
	claims := tkn.Claims.(jwt.MapClaims)
	claims["oid"] = "42"
	claims["name"] = "valid"
	expiresIn := 2 * time.Hour
	claims["exp"] = strconv.Itoa(int(time.Now().Add(expiresIn).Unix()))
	k, err := hex.DecodeString("DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF")
	if err != nil {
		log.Fatalf("i am not equipped to handle this!!! %v", err)
	}
	fresh, err := tkn.SignedString(k)
	if err != nil {
		log.Fatalf("i am not equipped to handle this!!! %v", err)
	}
	body := `{"access_token": "` + fresh + `", "refresh_token": "blah", "expires_in": "` + strconv.Itoa(int(expiresIn.Seconds())) + `"}`
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(body))
}

func main() {
	http.HandleFunc("/groups", groups)
	http.HandleFunc("/token", token)
	log.Println("FAKEIDP SERVER UP AND RUNNING")
	log.Fatal(http.ListenAndServe(":12345", nil))
}
