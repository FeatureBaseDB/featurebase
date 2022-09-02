// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package client

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	tests := []importLog{
		{
			Index: "go-testindex",
			Path:  "/index/go-testindex/field/importfield-batchsize/import?clear=false",
			Shard: 0,
			Data:  make([]byte, 3918),
		},
		{
			Index: "go-testindex",
			Path:  "/index/go-testindex/field/importfield-batchsize/import?clear=false",
			Shard: 0,
			Data:  make([]byte, 3918),
		},
		{
			Index: "eheh",
			Path:  "blah",
			Shard: 9,
			Data:  []byte("something"),
		},
		{
			Index: "",
			Path:  "",
			Shard: 0,
			Data:  nil,
		},
		{
			Index: "eheh",
			Path:  "blah",
			Shard: 10,
			Data:  []byte("blahaslkdjfeoiwujf"),
		},
		{
			Index: "eheh",
			Path:  "blah",
			Shard: 10,
			Data:  make([]byte, 10000),
		},
		{
			Index: "zoop",
			Path:  "blah",
			Shard: 8923734,
			Data:  []byte("blahaslkdjfeoiwujf"),
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			nl := importLog{
				Index: test.Index,
				Path:  test.Path,
				Shard: test.Shard,
				Data:  make([]byte, len(test.Data)),
			}
			copy(nl.Data, test.Data)
			buf := &bytes.Buffer{}
			enc := newImportLogEncoder(buf)
			err := enc.Encode(nl)
			if err != nil {
				t.Fatalf("writing to buf: %v", err)
			}
			dec := newImportLogDecoder(buf)
			l2 := &importLog{}
			err = dec.Decode(l2)
			if err != nil {
				t.Fatalf("reading from buf: %v", err)
			}
			if l2.Index != test.Index {
				t.Errorf("indexes not equal:\n%s\n%s", test.Index, l2.Index)
			}
			if l2.Path != test.Path {
				t.Errorf("paths not equal:\n%s\n%s", test.Path, l2.Path)
			}
			if l2.Shard != test.Shard {
				t.Errorf("shards not equal exp: %d got %d", test.Shard, l2.Shard)
			}
			if !reflect.DeepEqual(test.Data, l2.Data) {
				t.Errorf("data not equal \n%v\n%v", test.Data, l2.Data)
			}

		})
	}
	buf, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("getting temp file: %v", err)
	}
	enc := newImportLogEncoder(buf)
	for _, test := range tests {
		a := &test
		err := enc.Encode(a)
		if err != nil {
			t.Errorf("encoding to buf: %v", err)
		}
	}

	name := buf.Name()
	err = buf.Close()
	if err != nil {
		t.Fatalf("closing temp file: %v", err)
	}

	buf, err = os.Open(name)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}

	dec := newImportLogDecoder(buf)
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			l := &importLog{}
			err := dec.Decode(l)
			// err := l.ReadFrom(buf)
			if err != nil {
				t.Errorf("reading from buf: %v", err)
			}
			if l.Index != test.Index {
				t.Errorf("indexes not equal:\n%s\n%s", test.Index, l.Index)
			}
			if l.Path != test.Path {
				t.Errorf("paths not equal:\n%s\n%s", test.Path, l.Path)
			}
			if l.Shard != test.Shard {
				t.Errorf("shards not equal exp: %d got %d", test.Shard, l.Shard)
			}
			if !reflect.DeepEqual(test.Data, l.Data) {
				t.Errorf("data not equal \n%v\n%v", test.Data, l.Data)
			}
		})
	}
}
