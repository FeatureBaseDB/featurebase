// Copyright 2021 Molecula Corp. All rights reserved.
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	gohttp "net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/http"
	pnet "github.com/molecula/featurebase/v3/net"
	"github.com/molecula/featurebase/v3/vprint"
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

	profile string
	host    string
}

func (r *stateMachine) NewHeader(h *tar.Header, tr *tar.Reader) error {
	parts := strings.Split(h.Name, "/")
	switch parts[0] {
	case "roaring":
		index := parts[1]
		field := parts[2]
		view := parts[4]
		shard, err := strconv.ParseUint(parts[6], 10, 64)
		vprint.PanicOn(err)
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
			vprint.VV("Finished import %v", time.Since(r.start))

			if r.profile != "" {
				stopProfile(r.host, r.profile)
			}
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
			vprint.PanicOn(err)
			readerFunc := func() (io.Reader, error) {
				return bytes.NewReader(byteData), nil
			}
			err = r.client.ImportFieldKeys(context.Background(), uri, index, fieldName, false, readerFunc)
			if err != nil {
				return err
			}
		default:
			vprint.VV("%v", h.Name)
			index := parts[1]
			partition, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return err
			}
			byteData, err := ioutil.ReadAll(tr)
			vprint.PanicOn(err)
			readerFunc := func() (io.Reader, error) {
				return bytes.NewReader(byteData), nil
			}

			err = r.client.ImportIndexKeys(context.Background(), uri, index, int(partition), false, readerFunc)
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
			Views: r.viewData,
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

func UploadTar(srcFile string, client *http.InternalClient, profile, host string) error {

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
		profile:  profile,
		host:     host,
	}
	runner.client = client
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			_ = runner.Upload()
			break
		}
		if err != nil {
			vprint.PanicOn(err)
		}
		err = runner.NewHeader(header, tarReader)
		vprint.PanicOn(err)
	}
	return nil
}

func main() {
	var host string
	var profile string
	var tarSrcPath string
	flag.StringVar(&host, "host", "127.0.0.1:10101", "host to import into")
	flag.StringVar(&profile, "profile", "", "profile and save a cpu profile of the import to this file")
	flag.StringVar(&tarSrcPath, "src", "q2.tar.gz", "data to import")
	flag.Parse()

	uri, err := pnet.NewURIFromAddress(host)
	vprint.PanicOn(err)
	globURI = uri

	h := &gohttp.Client{}
	if profile != "" {
		startProfile(host)
	}
	c, err := http.NewInternalClient(host, h)
	vprint.PanicOn(err)

	t0 := time.Now()
	println("uploading", tarSrcPath)
	vprint.PanicOn(UploadTar(tarSrcPath, c, profile, host))
	vprint.VV("total elapsed '%v'", time.Since(t0))
}

func startProfile(host string) {
	cli := &gohttp.Client{}
	req := &gohttp.Request{
		Method: "GET",
		URL: &url.URL{
			Scheme: "http",
			Host:   host,
			Path:   "/cpu-profile/start",
		},
	}
	resp, err := cli.Do(req)
	if err != nil {
		if resp != nil {
			resp.Body.Close()
		}
		panic(err)
	}
}

func stopProfile(host, outfile string) {
	cli := &gohttp.Client{}
	req := &gohttp.Request{
		Method: "GET",
		URL: &url.URL{
			Scheme: "http",
			Host:   host,
			Path:   "/cpu-profile/stop",
		},
	}
	resp, err := cli.Do(req)
	if err != nil {
		if resp != nil {
			resp.Body.Close()
		}
		panic(err)
	}

	fd, err := os.Create(outfile)
	vprint.PanicOn(err)
	defer fd.Close()
	_, err = io.Copy(fd, resp.Body)
	vprint.PanicOn(err)

}

var globURI *pnet.URI

// get correct node to go to.
func GetImportRoaringURI(index string, shard uint64) *pnet.URI {
	return globURI
}
