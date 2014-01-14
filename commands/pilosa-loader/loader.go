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

func post(database, base_url, id, pid string) {
	values := make(url.Values)
	values.Set("db", database)
	values.Set("pql", fmt.Sprintf("set(%s,%s)", id, pid))
	r, err := http.PostForm(base_url, values)
	if err != nil {
		log.Printf("error posting stat to stathat: %s", err)
		return
	}
	body, _ := ioutil.ReadAll(r.Body)
	r.Body.Close()
	log.Println(body)
}

func Load(database, url, fullpath string) error {
	file, _ := os.Open(fullpath)
	reader := bufio.NewReader(file)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		recs := strings.Split(scanner.Text(), "|")
		//id, _ := strconv.ParseUint(recs[0], 10, 64) //brand
		id := recs[0]
		for _, profile_id := range recs[1:] {
			post(database, url, id, profile_id)

		}
	}
	return nil
}

// note, that variables are pointers
var database = flag.String("database", "main", "Database Name")
var host_port = flag.String("url", "127.0.0.1:8080", "pilosa point of entry host:port")
var file = flag.String("file", "input_file", "input file name")

func main() {
	flag.Parse()
	full_url := fmt.Sprintf("http://%s/query", *host_port)
	Load(*database, full_url, *file)
}
