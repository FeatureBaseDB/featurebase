package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
)

func post(database, base_url, id, pid, fragment_type string) {
	values := make(url.Values)
	values.Set("db", database)
	values.Set("pql", fmt.Sprintf("set(%s,%s,%s)", id, fragment_type, pid))
	r, err := http.PostForm(base_url, values)
	if err != nil {
		log.Printf("error posting stat to stathat: %s", err)
		return
	}
	//body, _ := ioutil.ReadAll(r.Body)
	ioutil.ReadAll(r.Body)
	r.Body.Close()
}

func Load(database, url, fullpath string, fragment_type string) error {
	f, err := os.Open(fullpath)
	if err != nil {
		fmt.Printf("error opening file: %v\n", err)
		os.Exit(1)
	}
	r := bufio.NewReader(f)
	line, e := Readln(r)
	for e == nil {
		recs := strings.Split(line, "|")
		id := recs[0]
		log.Println(id)
		for _, profile_id := range recs[1:] {
			post(database, url, id, profile_id, fragment_type)

		}
		line, e = Readln(r)
	}
	return nil
}

// note, that variables are pointers
var database = flag.String("database", "main", "Database Name")
var host_port = flag.String("url", "127.0.0.1:15001", "pilosa point of entry host:port")
var file = flag.String("file", "input_file", "input file name")
var fragment = flag.String("fragment", "brand", "Fragment Type")

func Readln(r *bufio.Reader) (string, error) {
	var (
		isPrefix bool  = true
		err      error = nil
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return string(ln), err
}

func main() {
	flag.Parse()
	full_url := fmt.Sprintf("http://%s/query", *host_port)
	//fun(*file)
	Load(*database, full_url, *file, *fragment)
}
