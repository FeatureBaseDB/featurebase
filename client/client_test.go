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

// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client

import (
	"crypto/tls"
	"errors"
	"reflect"
	"testing"

	pnet "github.com/pilosa/pilosa/v2/net"
)

func TestQueryWithError(t *testing.T) {
	var err error
	client := DefaultClient()
	index := NewIndex("foo")
	field := index.Field("foo")
	invalid := field.FilterAttrTopN(12, field.Row(7), "$invalid$", 80, 81)
	_, err = client.Query(invalid, nil)
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestClientOptions(t *testing.T) {
	targets := []*ClientOptions{
		{SocketTimeout: 10},
		{ConnectTimeout: 5},
		{PoolSizePerRoute: 7},
		{TotalPoolSize: 17},
		{TLSConfig: &tls.Config{InsecureSkipVerify: true}},
	}
	optionsList := [][]ClientOption{
		{OptClientSocketTimeout(10)},
		{OptClientConnectTimeout(5)},
		{OptClientPoolSizePerRoute(7)},
		{OptClientTotalPoolSize(17)},
		{OptClientTLSConfig(&tls.Config{InsecureSkipVerify: true})},
	}

	for i := 0; i < len(targets); i++ {
		options := &ClientOptions{}
		err := options.addOptions(optionsList[i]...)
		if err != nil {
			t.Fatal(err)
		}
		target := targets[i]
		if !reflect.DeepEqual(target, options) {
			t.Fatalf("%v != %v", target, options)
		}
	}
}

func TestNewClientWithErrorredOption(t *testing.T) {
	_, err := NewClient(":8888", ClientOptionErr(0))
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestNewClient(t *testing.T) {
	client, err := NewClient(":9999", OptClientManualServerAddress(true))
	if err != nil {
		t.Fatal(err)
	}
	targetURI, err := pnet.NewURIFromAddress(":9999")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(targetURI, client.manualServerURI) {
		t.Fatalf("%v != %v", targetURI, client.manualServerURI)
	}
	targetFragmentNode := &fragmentNode{
		Scheme: "http",
		Host:   "localhost",
		Port:   9999,
	}
	if !reflect.DeepEqual(targetFragmentNode, client.manualFragmentNode) {
		t.Fatalf("%v != %v", targetFragmentNode, client.manualFragmentNode)
	}
	client, err = NewClient(":9999")
	if err != nil {
		t.Fatal(err)
	}

	targetURI, err = pnet.NewURIFromAddress(":9999")
	if err != nil {
		t.Fatal(err)
	}

	target := []*pnet.URI{targetURI}
	if !reflect.DeepEqual(target, client.cluster.hosts) {
		t.Fatalf("%v != %v", target, client.cluster.hosts)
	}
	client, err = NewClient([]string{":9999"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(target, client.cluster.hosts) {
		t.Fatalf("%v != %v", target, client.cluster.hosts)
	}

	targetURI1, err := pnet.NewURIFromAddress(":8888")
	if err != nil {
		t.Fatal(err)
	}
	targetURI2, err := pnet.NewURIFromAddress(":9999")
	if err != nil {
		t.Fatal(err)
	}

	client, err = NewClient([]*pnet.URI{targetURI1, targetURI2})
	if err != nil {
		t.Fatal(err)
	}

	target = []*pnet.URI{targetURI1, targetURI2}
	if !reflect.DeepEqual(target, client.cluster.hosts) {
		t.Fatalf("%v != %v", target, client.cluster.hosts)
	}

	client, err = NewClient([]*pnet.URI{targetURI})
	if err != nil {
		t.Fatal(err)
	}
	target = []*pnet.URI{targetURI}
	if !reflect.DeepEqual(target, client.cluster.hosts) {
		t.Fatalf("%v != %v", target, client.cluster.hosts)
	}

	client, err = NewClient(DefaultCluster())
	if err != nil {
		t.Fatal(err)
	}
	target = []*pnet.URI{}
	if !reflect.DeepEqual(target, client.cluster.hosts) {
		t.Fatalf("%v != %v", target, client.cluster.hosts)
	}
}

func TestNewClientWithInvalidAddr(t *testing.T) {
	_, err := NewClient(10)
	if err != ErrAddrURIClusterExpected {
		t.Fatalf("%v != %v", ErrAddrURIClusterExpected, err)
	}
	_, err = NewClient(":invalid")
	if err == nil {
		t.Fatalf("should have failed: %+v", err)
	}
	_, err = NewClient([]string{"valid:8000", ":invalid"})
	if err != pnet.ErrInvalidAddress {
		t.Fatalf("Should have failed '%v, got '%v'", pnet.ErrInvalidAddress, err)
	}
}

func TestNewClientManualAddressWithNoURIs(t *testing.T) {
	_, err := NewClient([]string{}, OptClientManualServerAddress(true))
	if err != ErrSingleServerAddressRequired {
		t.Fatalf("%v != %v", ErrSingleServerAddressRequired, err)
	}
	_, err = NewClient([]*pnet.URI{}, OptClientManualServerAddress(true))
	if err != ErrSingleServerAddressRequired {
		t.Fatalf("%v != %v", ErrSingleServerAddressRequired, err)
	}
}

func TestNewClientManualAddressWithMultipleURIs(t *testing.T) {
	_, err := NewClient([]string{":9000", ":5000"}, OptClientManualServerAddress(true))
	if err != ErrSingleServerAddressRequired {
		t.Fatalf("%v != %v", ErrSingleServerAddressRequired, err)
	}

	targetURI1, err := pnet.NewURIFromAddress(":9000")
	if err != nil {
		t.Fatal(err)
	}
	targetURI2, err := pnet.NewURIFromAddress(":5000")
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewClient([]*pnet.URI{targetURI1, targetURI2}, OptClientManualServerAddress(true))
	if err != ErrSingleServerAddressRequired {
		t.Fatalf("%v != %v", ErrSingleServerAddressRequired, err)
	}
}

func ClientOptionErr(int) ClientOption {
	return func(*ClientOptions) error {
		return errors.New("Some error")
	}
}

func TestQueryOptions(t *testing.T) {
	targets := []*QueryOptions{
		{ColumnAttrs: true},
		{ColumnAttrs: false},
		{ExcludeRowAttrs: true},
		{ExcludeRowAttrs: false},
		{ExcludeColumns: true},
		{ExcludeColumns: false},
	}

	optionsList := [][]interface{}{
		{OptQueryColumnAttrs(true)},
		{OptQueryColumnAttrs(false)},
		{OptQueryExcludeAttrs(true)},
		{OptQueryExcludeAttrs(false)},
		{OptQueryExcludeColumns(true)},
		{OptQueryExcludeColumns(false)},
	}

	for i := 0; i < len(targets); i++ {
		options := &QueryOptions{}
		err := options.addOptions(optionsList[i]...)
		if err != nil {
			t.Fatal(err)
		}
		target := targets[i]
		if !reflect.DeepEqual(target, options) {
			t.Fatalf("%v != %v", target, options)
		}
	}

	target := &QueryOptions{
		ColumnAttrs:     true,
		ExcludeRowAttrs: true,
		ExcludeColumns:  true,
	}
	options := &QueryOptions{}
	err := options.addOptions(&QueryOptions{
		ColumnAttrs:     true,
		ExcludeRowAttrs: true,
		ExcludeColumns:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(target, options) {
		t.Fatalf("%v != %v", target, options)
	}
}

func TestQueryOptionsWithError(t *testing.T) {
	options := &QueryOptions{}
	err := options.addOptions(1)
	if err == nil {
		t.Fatalf("should have failed")
	}
	err = options.addOptions(OptQueryColumnAttrs(true), nil)
	if err == nil {
		t.Fatalf("should have failed")
	}
	err = options.addOptions(OptQueryColumnAttrs(true), &QueryOptions{})
	if err == nil {
		t.Fatalf("should have failed")
	}
	err = options.addOptions(QueryOptionErr(0))
	if err == nil {
		t.Fatalf("should have failed")
	}
}

func TestQueryOptionsError(t *testing.T) {
	client := DefaultClient()
	index := NewIndex("foo")
	_, err := client.Query(index.RawQuery(""), QueryOptionErr(0))
	if err == nil {
		t.Fatalf("should have failed")
	}
}

func QueryOptionErr(int) QueryOption {
	return func(*QueryOptions) error {
		return errors.New("Some error")
	}
}
