package main

import (
	"encoding/hex"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt"
)

func groups(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"value":[{"id":"group-id-test","displayName":"group-id-test"}]}`))
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
	log.Fatal(http.ListenAndServe(":10101", nil))
}
