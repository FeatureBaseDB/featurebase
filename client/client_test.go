// Copyright 2021 Molecula Corp. All rights reserved.
// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client

import (
	"crypto/tls"
	"errors"
	"reflect"
	"testing"

	pnet "github.com/molecula/featurebase/v2/net"
)

func TestQueryWithError(t *testing.T) {
	var err error
	client := DefaultClient()
	index := NewIndex("foo")
	invalid := NewPQLRowQuery("", index, errors.New("invalid"))
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
