// Copyright 2017 Pilosa Corp.
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

package synthload

/* // still work in progress, comment out for now, since we
   // are getting a hang on go1.13 in CI
import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/server"
	"github.com/molecula/featurebase/v2/test"
)

func Test_SynthLoad_ImportSchema(t *testing.T) {
	c := test.MustRunCluster(t, 1,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node0"),
			)},
	)
	defer c.Close()

	m0 := c.GetNode(0)

	tarball := "testindex.tar.gz"

	// get the holder.Path to write to
	h := m0.API.Holder()
	target := h.Path()
	PanicOn(h.Close())

	PanicOn(unpackTarball(tarball, target))

	// reopen
	PanicOn(h.Open())

	qs := strings.Split(pql, "\n\n")
	//vv("qs = '%#v'", qs)
	for _, q := range qs {
		req := &pilosa.QueryRequest{
			// Index to execute query against.
			Index: "testindex",

			// The query string to parse and execute.
			Query: q,

			// // The shards to include in the query execution.
			// // If empty, all shards are included.
			// Shards []uint64

			// // Return column attributes, if true.
			// ColumnAttrs bool

			// // Do not return row attributes, if true.
			// ExcludeRowAttrs bool

			// // Do not return columns, if true.
			// ExcludeColumns bool

			// // If true, indicates that query is part of a larger distributed query.
			// // If false, this request is on the originating node.
			// Remote bool

			// // Should we profile this query?
			// Profile bool

			// // Additional data associated with the query, in cases where there's
			// // row-style inputs for precomputed values.
			// EmbeddedData []*Row
		}

		qr, err := m0.API.Query(context.Background(), req)
		PanicOn(err)
		vv("qr = '%#v'", qr)
	}
}

var _ = applySchema

func applySchema(m0 *test.Command, schemaStr string) {
	// don't need schema now that we import the tarball, it has it all.
	schema := &pilosa.Schema{}
	err := json.NewDecoder(bytes.NewBufferString(schemaStr)).Decode(schema)
	PanicOn(err)

	ctx := context.Background()
	remote := false
	err = m0.API.ApplySchema(ctx, schema, remote)
	PanicOn(err)
}

func unpackTarball(tarball, target string) error {

	vv("target = '%v'", target)
	fd, err := os.Open(tarball)
	PanicOn(err)
	defer fd.Close()
	gz, err := gzip.NewReader(fd)
	PanicOn(err)
	defer gz.Close()

	tarReader := tar.NewReader(gz)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		path := filepath.Join(target, header.Name)
		info := header.FileInfo()
		if info.IsDir() {
			if err = os.MkdirAll(path, info.Mode()); err != nil {
				PanicOn(err)
			}
			continue
		}

		file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
		PanicOn(err)

		_, err = io.Copy(file, tarReader)
		PanicOn(err)
		file.Close()
	}
	return nil
}

// mkdir o; bangbang schemator -o o
var pql = `
Count(All())

Rows(field='bools',limit=10)
Count(Row(bools='bool'))
Count(Not(Row(bools='bool')))
Count(Intersect(Row(bools='bool')))
Count(Difference(Union(Row(bools='bool')),Intersect(Row(bools='bool'))))
GroupBy(Rows(field='bools'),limit=10)

Rows(field='bools-exists',limit=10)
Count(Row(bools-exists='bool'))
Count(Not(Row(bools-exists='bool')))
Count(Intersect(Row(bools-exists='bool')))
Count(Difference(Union(Row(bools-exists='bool')),Intersect(Row(bools-exists='bool'))))
GroupBy(Rows(field='bools-exists'),limit=10)

GroupBy(Rows(field='int'),limit=10)
Min(field='int')
Max(field='int')
Sum(field='int')

Rows(field='mutex',limit=10)
Count(Row(mutex='12'))
Count(Not(Row(mutex='12')))
Count(Row(mutex='13'))
Count(Not(Row(mutex='13')))
Count(Row(mutex='0'))
Count(Not(Row(mutex='0')))
Count(Row(mutex='5'))
Count(Not(Row(mutex='5')))
Count(Row(mutex='2'))
Count(Not(Row(mutex='2')))
Count(Row(mutex='14'))
Count(Not(Row(mutex='14')))
Count(Row(mutex='3'))
Count(Not(Row(mutex='3')))
Count(Row(mutex='18'))
Count(Not(Row(mutex='18')))
Count(Row(mutex='7'))
Count(Not(Row(mutex='7')))
Count(Row(mutex='19'))
Count(Not(Row(mutex='19')))
Count(Intersect(Row(mutex='12'),Row(mutex='13'),Row(mutex='0'),Row(mutex='5'),Row(mutex='2'),Row(mutex='14'),Row(mutex='3'),Row(mutex='18'),Row(mutex='7'),Row(mutex='19')))
Count(Difference(Union(Row(mutex='12'),Row(mutex='13'),Row(mutex='0'),Row(mutex='5'),Row(mutex='2'),Row(mutex='14'),Row(mutex='3'),Row(mutex='18'),Row(mutex='7'),Row(mutex='19')),Intersect(Row(mutex='12'),Row(mutex='13'),Row(mutex='0'),Row(mutex='5'),Row(mutex='2'),Row(mutex='14'),Row(mutex='3'),Row(mutex='18'),Row(mutex='7'),Row(mutex='19'))))
GroupBy(Rows(field='mutex'),limit=10)

Rows(field='set',limit=10)
Count(Row(set='v621'))
Count(Not(Row(set='v621')))
Count(Row(set='v997'))
Count(Not(Row(set='v997')))
Count(Row(set='v772'))
Count(Not(Row(set='v772')))
Count(Row(set='v340'))
Count(Not(Row(set='v340')))
Count(Row(set='v766'))
Count(Not(Row(set='v766')))
Count(Row(set='v416'))
Count(Not(Row(set='v416')))
Count(Row(set='v481'))
Count(Not(Row(set='v481')))
Count(Row(set='v581'))
Count(Not(Row(set='v581')))
Count(Row(set='v591'))
Count(Not(Row(set='v591')))
Count(Row(set='v675'))
Count(Not(Row(set='v675')))
Count(Intersect(Row(set='v621'),Row(set='v997'),Row(set='v772'),Row(set='v340'),Row(set='v766'),Row(set='v416'),Row(set='v481'),Row(set='v581'),Row(set='v591'),Row(set='v675')))
Count(Difference(Union(Row(set='v621'),Row(set='v997'),Row(set='v772'),Row(set='v340'),Row(set='v766'),Row(set='v416'),Row(set='v481'),Row(set='v581'),Row(set='v591'),Row(set='v675')),Intersect(Row(set='v621'),Row(set='v997'),Row(set='v772'),Row(set='v340'),Row(set='v766'),Row(set='v416'),Row(set='v481'),Row(set='v581'),Row(set='v591'),Row(set='v675'))))
GroupBy(Rows(field='set'),limit=10)

Rows(field='string',limit=10)
Count(Row(string='KPFGWOYUGTCF'))
Count(Not(Row(string='KPFGWOYUGTCF')))
Count(Row(string='VNSHWDTNELDA'))
Count(Not(Row(string='VNSHWDTNELDA')))
Count(Row(string='LTDOGKKFGZPW'))
Count(Not(Row(string='LTDOGKKFGZPW')))
Count(Row(string='EETNIIDCDZHB'))
Count(Not(Row(string='EETNIIDCDZHB')))
Count(Row(string='ATWOMTRASGHP'))
Count(Not(Row(string='ATWOMTRASGHP')))
Count(Row(string='KOJUBJQVMXZL'))
Count(Not(Row(string='KOJUBJQVMXZL')))
Count(Row(string='NOXDEYPTGZBH'))
Count(Not(Row(string='NOXDEYPTGZBH')))
Count(Row(string='DLSRPRADBPKX'))
Count(Not(Row(string='DLSRPRADBPKX')))
Count(Row(string='WJUEQABAEEUX'))
Count(Not(Row(string='WJUEQABAEEUX')))
Count(Row(string='PFRGBFDLHHPK'))
Count(Not(Row(string='PFRGBFDLHHPK')))
Count(Intersect(Row(string='KPFGWOYUGTCF'),Row(string='VNSHWDTNELDA'),Row(string='LTDOGKKFGZPW'),Row(string='EETNIIDCDZHB'),Row(string='ATWOMTRASGHP'),Row(string='KOJUBJQVMXZL'),Row(string='NOXDEYPTGZBH'),Row(string='DLSRPRADBPKX'),Row(string='WJUEQABAEEUX'),Row(string='PFRGBFDLHHPK')))
Count(Difference(Union(Row(string='KPFGWOYUGTCF'),Row(string='VNSHWDTNELDA'),Row(string='LTDOGKKFGZPW'),Row(string='EETNIIDCDZHB'),Row(string='ATWOMTRASGHP'),Row(string='KOJUBJQVMXZL'),Row(string='NOXDEYPTGZBH'),Row(string='DLSRPRADBPKX'),Row(string='WJUEQABAEEUX'),Row(string='PFRGBFDLHHPK')),Intersect(Row(string='KPFGWOYUGTCF'),Row(string='VNSHWDTNELDA'),Row(string='LTDOGKKFGZPW'),Row(string='EETNIIDCDZHB'),Row(string='ATWOMTRASGHP'),Row(string='KOJUBJQVMXZL'),Row(string='NOXDEYPTGZBH'),Row(string='DLSRPRADBPKX'),Row(string='WJUEQABAEEUX'),Row(string='PFRGBFDLHHPK'))))
GroupBy(Rows(field='string'),limit=10)

GroupBy(Rows(field='time'),limit=10)
`

//  datagen --source=kitchensink --pilosa.index=testindex --start-from=1 --end-at=1000 --pilosa.batch-size=10000 --pilosa.hosts localhost:10101
// curl localhost:10101/schema

var _ = schemaString
var schemaString = `
{
  "indexes": [
    {
      "name": "testindex",
      "createdAt": 1599601704641744600,
      "options": {
        "keys": false,
        "trackExistence": true
      },
      "fields": [
        {
          "name": "bools",
          "createdAt": 1599601704647053000,
          "options": {
            "type": "set",
            "cacheType": "ranked",
            "cacheSize": 50000,
            "keys": true
          }
        },
        {
          "name": "bools-exists",
          "createdAt": 1599601704643225900,
          "options": {
            "type": "set",
            "cacheType": "ranked",
            "cacheSize": 50000,
            "keys": true
          }
        },
        {
          "name": "decimal",
          "createdAt": 1599601704642064400,
          "options": {
            "type": "decimal",
            "base": 0,
            "scale": 2,
            "bitDepth": 63,
            "min": -92233720368547760,
            "max": 92233720368547760,
            "keys": false
          }
        },
        {
          "name": "int",
          "createdAt": 1599601704645510100,
          "options": {
            "type": "int",
            "base": 0,
            "bitDepth": 63,
            "min": -9223372036854776000,
            "max": 9223372036854776000,
            "keys": false,
            "foreignIndex": ""
          }
        },
        {
          "name": "mutex",
          "createdAt": 1599601704647187200,
          "options": {
            "type": "mutex",
            "cacheType": "ranked",
            "cacheSize": 500,
            "keys": true
          }
        },
        {
          "name": "set",
          "createdAt": 1599601704643664400,
          "options": {
            "type": "set",
            "cacheType": "lru",
            "cacheSize": 1,
            "keys": true
          }
        },
        {
          "name": "string",
          "createdAt": 1599601704644342000,
          "options": {
            "type": "set",
            "cacheType": "ranked",
            "cacheSize": 50000,
            "keys": true
          }
        },
        {
          "name": "time",
          "createdAt": 1599601704643013400,
          "options": {
            "type": "time",
            "timeQuantum": "YMD",
            "keys": true,
            "noStandardView": false
          }
        }
      ],
      "shardWidth": 1048576
    }
  ]
}
`

*/
