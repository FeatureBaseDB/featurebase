// Copyright 2020 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"time"

	//"fmt"
	"fmt"
	"io"
	"io/ioutil"
	gohttp "net/http"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/http"

	//"log"
	"os"
	//"path/filepath"
	//"sort"
	"strconv"
	"strings"
)

// slurp: slurp is a load-tester for importing bulk data.
//        It allows us to measure write performance.

type stateMachine struct {
	viewData  map[string][]byte
	lastIndex string
	lastField string
	lastShard uint64
	state     string
	client    *http.InternalClient
	start     time.Time
	direct    bool
}

func (r *stateMachine) NewHeader(h *tar.Header, tr *tar.Reader) error {
	parts := strings.Split(h.Name, "/")
	switch parts[0] {
	case "roaring":
		index := parts[1]
		field := parts[2]
		view := parts[4]
		shard, err := strconv.ParseUint(parts[6], 10, 64)
		panicOn(err)
		if index != r.lastIndex || field != r.lastField || shard != r.lastShard {
			err := r.Upload()
			if err != nil {
				return err
			}
		}
		roaringData, err := ioutil.ReadAll(tr)
		if err != nil {
			return err
		}
		if _, already := r.viewData[view]; already {
			panic(fmt.Sprintf("view '%v' already present!", view))
		}
		r.viewData[view] = roaringData
		r.lastIndex = index
		r.lastField = field
		r.lastShard = shard
	case "bolt":
		if r.state == "roaring" {
			err := r.Upload()
			if err != nil {
				return err
			}
			vv("Finished import %v", time.Since(r.start))
		}
		//
		uri := GetImportRoaringURI(r.lastIndex, r.lastShard)

		switch v := parts[len(parts)-1]; v {
		case "keys":
			index := parts[1]
			fieldName := parts[2]
			if fieldName == "_keys" {
				//skip index keys are not not real fields so will have no need for field keys
				return nil

			}

			byteData, err := ioutil.ReadAll(tr)
			panicOn(err)
			br := bytes.NewReader(byteData)
			err = r.client.ImportFieldKeys(context.Background(), uri, index, fieldName, false, br)
			if err != nil {
				return err
			}
		default:
			pilosa.VV("%v", h.Name)
			index := parts[1]
			partition, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return err
			}
			byteData, err := ioutil.ReadAll(tr)
			panicOn(err)

			br := bytes.NewReader(byteData)
			err = r.client.ImportIndexKeys(context.Background(), uri, index, int(partition), false, br)
			if err != nil {
				return err
			}
		}
	}
	r.state = parts[0]
	return nil
}
func (r *stateMachine) Upload() error {
	if len(r.viewData) > 0 {
		request := &pilosa.ImportRoaringRequest{
			Views:  r.viewData,
			Direct: r.direct,
		}
		uri := GetImportRoaringURI(r.lastIndex, r.lastShard)
		err := r.client.ImportRoaring(context.Background(), uri, r.lastIndex, r.lastField, r.lastShard, false, request)
		if err != nil {
			return err
		}
		r.viewData = make(map[string][]byte)
	}
	return nil
}

func UploadTar(srcFile string, direct bool, client *http.InternalClient) error {

	f, err := os.Open(srcFile)
	if err != nil {
		return (err)
	}
	defer f.Close()
	var tarReader *tar.Reader
	if strings.HasSuffix(srcFile, "gz") {
		gzf, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		tarReader = tar.NewReader(gzf)
	} else {
		tarReader = tar.NewReader(f)
	}
	runner := &stateMachine{
		viewData: make(map[string][]byte),
		start:    time.Now(),
		direct:   direct,
	}
	runner.client = client
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			_ = runner.Upload()
			break
		}
		if err != nil {
			panicOn(err)
		}
		err = runner.NewHeader(header, tarReader)
		panicOn(err)
	}
	return nil
}

func main() {
	var host string
	var direct bool
	var tarSrcPath string
	flag.StringVar(&host, "host", "127.0.0.1:10101", "host to import into")
	flag.BoolVar(&direct, "direct", false, "direct write to database (unsafe)")
	flag.StringVar(&tarSrcPath, "src", "q2.tar.gz", "data to import")
	flag.Parse()

	uri, err := pilosa.NewURIFromAddress(host)
	panicOn(err)
	globURI = uri

	h := &gohttp.Client{}
	c, err := http.NewInternalClient(host, h)
	panicOn(err)

	t0 := time.Now()
	println("uploading", tarSrcPath)
	panicOn(UploadTar(tarSrcPath, direct, c))
	vv("total elapsed '%v'", time.Since(t0))
}

var globURI *pilosa.URI

// get correct node to go to.
func GetImportRoaringURI(index string, shard uint64) *pilosa.URI {
	return globURI
}
