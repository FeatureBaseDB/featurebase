// Copyright 2021 Molecula Corp. All rights reserved.
package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"time"

	"fmt"
	"io"
	"io/ioutil"
	gohttp "net/http"

	pilosa "github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/http"
	pnet "github.com/molecula/featurebase/v2/net"
	"github.com/molecula/featurebase/v2/vprint"

	"os"
	"strconv"
	"strings"
)

func UploadTar(srcFile string, client *http.InternalClient) error {
	t0 := time.Now()
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
	viewData := make(map[string][]byte)
	//given ordered by index/field/view
	//trait_store/product_count__commercial_cd_or_share_certificate/views/bsig_product_count__commercial_cd_or_share_certificate/fragments/255
	lastIndex := ""
	lastField := ""
	lastShard := uint64(0)
	n := 0
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			if header != nil {
				vprint.PanicOn("header should not be nil on err io.EOF")
			}
			//submit any stuff we have left
			if len(viewData) > 0 {
				request := &pilosa.ImportRoaringRequest{
					Views: viewData,
				}
				//	Submit(lastIndex, lastField, lastShard, request)
				uri := GetImportRoaringURI(lastIndex, lastShard)
				err := client.ImportRoaring(context.Background(), uri, lastIndex, lastField, lastShard, false, request)
				vprint.PanicOn(err)
			}
			return nil
		}
		n++
		if n%500 == 0 {
			vprint.VV("n = %v, progress, elapsed '%v'", n, time.Since(t0))
		}
		parts := strings.Split(header.Name, "/")
		//vv("parts = '%#v'", parts)
		index := parts[1]
		field := parts[2]
		view := parts[4]
		shard, err := strconv.ParseUint(parts[6], 10, 64)
		if err != nil {
			return err
		}
		// TODO: shards can be loaded in parallel, so maybe farm out to a worker set of goro.
		if index != lastIndex || field != lastField || shard != lastShard {
			if len(viewData) > 0 {
				request := &pilosa.ImportRoaringRequest{
					Views: viewData,
				}
				//vv("about to submit lastIndex='%v' lastShard='%v'", lastIndex, lastShard)
				uri := GetImportRoaringURI(lastIndex, lastShard)
				vprint.PanicOn(client.ImportRoaring(context.Background(), uri, lastIndex, lastField, lastShard, false, request))
				viewData = make(map[string][]byte)
				//vv("done with submit lastIndex='%v' lastShard='%v'; took='%v'", lastIndex, lastShard, time.Since(t0))

			}
		}
		roaringData, err := ioutil.ReadAll(tarReader)
		if err != nil {
			return err
		}
		if _, already := viewData[view]; already {
			vprint.PanicOn(fmt.Sprintf("view '%v' already present!", view))
		}
		viewData[view] = roaringData
		lastIndex = index
		lastField = field

		//lastShard = shard
		//vv("bottom of loop")
	}
}

// badloader reproduce a union in place issue for us. slurp is
// the new "good" loader, and should always be preferred now
// when not trying to repro that bug. pulled from 85fa67e8
func main() {

	host := "127.0.0.1:10101"
	h := &gohttp.Client{}
	c, err := http.NewInternalClient(host, h)
	vprint.PanicOn(err)

	tarSrcPath := "q2.tar.gz"
	t0 := time.Now()
	vprint.PanicOn(UploadTar(tarSrcPath, c))
	vprint.VV("total elapsed '%v'", time.Since(t0))
}

var globURI *pnet.URI

func init() {
	var err error
	globURI, err = pnet.NewURIFromHostPort("127.0.0.1", 10101)
	vprint.PanicOn(err)
}

// get correct node to go to.
func GetImportRoaringURI(index string, shard uint64) *pnet.URI {
	return globURI
}
