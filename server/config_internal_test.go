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

package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v2/auth"
)

type addrs struct{ bind, advertise string }

func TestConfig_validateAddrs(t *testing.T) {
	// Prepare some reference strings that will be checked in the
	// test below.
	outboundAddr := outboundIP().String()
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	hostAddr, err := lookupAddr(context.Background(), net.DefaultResolver, hostname)
	if err != nil {
		if !strings.HasSuffix(hostname, ".local") {
			t.Fatal(err)
		}
		hostAddr = outboundAddr
	}
	if strings.Contains(hostAddr, ":") {
		hostAddr = "[" + hostAddr + "]"
	}

	tests := []struct {
		expErr string
		in     addrs
		exp    addrs
	}{
		// Default values; addresses set empty.
		{"",
			addrs{"", ""},
			addrs{":10101", ":10101"}},
		{"",
			addrs{":", ""},
			addrs{":10101", ":10101"}},
		{"",
			addrs{"", ":"},
			addrs{":10101", ":10101"}},
		{"",
			addrs{":", ":"},
			addrs{":10101", ":10101"}},
		// Listener :port.
		{"",
			addrs{":1234", ""},
			addrs{":1234", ":1234"}},
		// Listener with host:port.
		{"",
			addrs{hostAddr + ":10101", ""},
			addrs{hostAddr + ":10101", hostAddr + ":10101"}},
		// Listener with host:.
		{"",
			addrs{hostAddr + ":", ""},
			addrs{hostAddr + ":10101", hostAddr + ":10101"}},
		// Listener with scheme:.
		{"",
			addrs{"http://" + hostAddr + ":", ""},
			addrs{"http://" + hostAddr + ":10101", "http://" + hostAddr + ":10101"}},
		// Listener with localhost:port.
		{"",
			addrs{"localhost:1234", ""},
			addrs{"localhost:1234", "localhost:1234"}},
		// Listener with localhost:.
		{"",
			addrs{"localhost:", ""},
			addrs{"localhost:10101", "localhost:10101"}},
		// Listener and advertise addresses.
		{"",
			addrs{hostAddr + ":1234", hostAddr + ":"},
			addrs{hostAddr + ":1234", hostAddr + ":1234"}},
		// Explicit port number in advertise addr.
		{"",
			addrs{hostAddr + ":1234", hostAddr + ":7890"},
			addrs{hostAddr + ":1234", hostAddr + ":7890"}},
		// Use a non-numeric port number.
		{"",
			addrs{":postgresql", ""},
			addrs{":5432", ":5432"}},
		// Advertise port 0 means reuse listen port.
		{"",
			addrs{":1234", ":0"},
			addrs{":1234", ":1234"}},
		// Listen on all interfaces. Determine advertise address.
		{"",
			addrs{"0.0.0.0:1234", ""},
			addrs{"0.0.0.0:1234", outboundAddr + ":1234"}},

		// Expected errors.
		//
		// Missing port number.
		{"missing port in address",
			addrs{"localhost", ""},
			addrs{}},
		{"missing port in address",
			addrs{":1234", "localhost"},
			addrs{}},
		// Invalid port number.
		{"invalid port",
			addrs{"localhost:-1234", ""},
			addrs{}},
		{"validating advertise address",
			addrs{"localhost:foo", ""},
			addrs{}},
		{"no such host",
			addrs{"333.333.333.333:1234", ""},
			addrs{}},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			c := NewConfig()

			c.Bind = test.in.bind
			c.Advertise = test.in.advertise

			err := c.validateAddrs(context.Background())

			if err != nil && test.expErr == "" {
				t.Fatal(err)
			} else if err == nil && test.expErr != "" {
				t.Fatalf("expected error string to contain %s, but got no error", test.expErr)
			} else if err != nil && test.expErr != "" {
				if !strings.Contains(err.Error(), test.expErr) {
					t.Fatalf("expected error string to contain %s, but got %s", test.expErr, err.Error())
				}
				return
			}

			if c.Bind != test.exp.bind {
				t.Fatalf("bind address: expected %s, but got %s", test.exp.bind, c.Bind)
			} else if c.Advertise != test.exp.advertise {
				t.Fatalf("advertise address: expected %s, but got %s", test.exp.advertise, c.Advertise)
			}
		})
	}
}

func TestConfig_validateAddrsGRPC(t *testing.T) {
	// Prepare some reference strings that will be checked in the
	// test below.
	outboundAddr := outboundIP().String()
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	hostAddr, err := lookupAddr(context.Background(), net.DefaultResolver, hostname)
	if err != nil {
		if !strings.HasSuffix(hostname, ".local") {
			t.Fatal(err)
		}
		hostAddr = outboundAddr
	}
	if strings.Contains(hostAddr, ":") {
		hostAddr = "[" + hostAddr + "]"
	}

	tests := []struct {
		expErr string
		in     addrs
		exp    addrs
	}{
		// Default values; addresses set empty.
		{"",
			addrs{"", ""},
			addrs{"grpc://:20101", "grpc://:20101"}},
		{"",
			addrs{":", ""},
			addrs{"grpc://:20101", "grpc://:20101"}},
		{"",
			addrs{"", ":"},
			addrs{"grpc://:20101", "grpc://:20101"}},
		{"",
			addrs{":", ":"},
			addrs{"grpc://:20101", "grpc://:20101"}},
		// Listener :port.
		{"",
			addrs{":1234", ""},
			addrs{"grpc://:1234", "grpc://:1234"}},
		// Listener with host:port.
		{"",
			addrs{hostAddr + ":20101", ""},
			addrs{"grpc://" + hostAddr + ":20101", "grpc://" + hostAddr + ":20101"}},
		// Listener with host:.
		{"",
			addrs{hostAddr + ":", ""},
			addrs{"grpc://" + hostAddr + ":20101", "grpc://" + hostAddr + ":20101"}},
		// Listener with scheme:.
		{"",
			addrs{"http://" + hostAddr + ":", ""},
			addrs{"grpc://" + hostAddr + ":20101", "grpc://" + hostAddr + ":20101"}},
		// Listener with localhost:port.
		{"",
			addrs{"localhost:1234", ""},
			addrs{"grpc://localhost:1234", "grpc://localhost:1234"}},
		// Listener with localhost:.
		{"",
			addrs{"localhost:", ""},
			addrs{"grpc://localhost:20101", "grpc://localhost:20101"}},
		// Listener and advertise addresses.
		{"",
			addrs{hostAddr + ":1234", hostAddr + ":"},
			addrs{"grpc://" + hostAddr + ":1234", "grpc://" + hostAddr + ":1234"}},
		// Explicit port number in advertise addr.
		{"",
			addrs{hostAddr + ":1234", hostAddr + ":7890"},
			addrs{"grpc://" + hostAddr + ":1234", "grpc://" + hostAddr + ":7890"}},
		// Use a non-numeric port number.
		{"",
			addrs{":postgresql", ""},
			addrs{"grpc://:5432", "grpc://:5432"}},
		// Advertise port 0 means reuse listen port.
		{"",
			addrs{":1234", ":0"},
			addrs{"grpc://:1234", "grpc://:1234"}},
		// Listen on all interfaces. Determine advertise address.
		{"",
			addrs{"0.0.0.0:1234", ""},
			addrs{"grpc://0.0.0.0:1234", "grpc://" + outboundAddr + ":1234"}},

		// Expected errors.
		//
		// Missing port number.
		{"missing port in address",
			addrs{"localhost", ""},
			addrs{}},
		{"missing port in address",
			addrs{":1234", "localhost"},
			addrs{}},
		// Invalid port number.
		{"invalid port",
			addrs{"localhost:-1234", ""},
			addrs{}},
		{"validating grpc advertise address",
			addrs{"localhost:foo", ""},
			addrs{}},
		{"no such host",
			addrs{"333.333.333.333:1234", ""},
			addrs{}},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			c := NewConfig()

			c.BindGRPC = test.in.bind
			c.AdvertiseGRPC = test.in.advertise

			err := c.validateAddrs(context.Background())

			if err != nil && test.expErr == "" {
				t.Fatal(err)
			} else if err == nil && test.expErr != "" {
				t.Fatalf("expected error string to contain %s, but got no error", test.expErr)
			} else if err != nil && test.expErr != "" {
				if !strings.Contains(err.Error(), test.expErr) {
					t.Fatalf("expected error string to contain %s, but got %s", test.expErr, err.Error())
				}
				return
			}

			if c.BindGRPC != test.exp.bind {
				t.Fatalf("bind address: expected %s, but got %s", test.exp.bind, c.BindGRPC)
			} else if c.AdvertiseGRPC != test.exp.advertise {
				t.Fatalf("advertise address: expected %s, but got %s", test.exp.advertise, c.AdvertiseGRPC)
			}
		})
	}
}

func TestConfig_validateAuth(t *testing.T) {
	errorMesgEmpty := "Empty string"
	errorMesgURL := "Invalid URL"
	validTestURL := "https://url.com/"
	validClientID := "clientid"
	validClientSecret := "clientSecret"
	notValidURL := "not-a-url"
	emptyString := ""
	enable := true
	disable := false

	tests := []struct {
		expErrs []string
		input   auth.Auth
	}{

		{
			// Auth enabled, all configs are set to empty string
			[]string{
				errorMesgEmpty,
				errorMesgEmpty,
				errorMesgEmpty,
				errorMesgEmpty,
				errorMesgEmpty,
				errorMesgEmpty,
			},
			auth.Auth{
				Enable:           enable,
				ClientId:         emptyString,
				ClientSecret:     emptyString,
				AuthorizeURL:     emptyString,
				TokenURL:         emptyString,
				GroupEndpointURL: emptyString,
				ScopeURL:         emptyString,
			},
		},
		{
			// Auth enabled, some configs are set to empty string
			[]string{
				errorMesgEmpty,
				errorMesgEmpty,
				errorMesgEmpty,
				errorMesgEmpty,
				errorMesgEmpty,
			},
			auth.Auth{
				Enable:           enable,
				ClientId:         validClientID,
				ClientSecret:     emptyString,
				AuthorizeURL:     emptyString,
				TokenURL:         emptyString,
				GroupEndpointURL: emptyString,
				ScopeURL:         emptyString,
			},
		},
		{
			// Auth enabled, some configs are set to empty string
			[]string{
				errorMesgEmpty,
				errorMesgEmpty,
				errorMesgEmpty,
				errorMesgEmpty,
				errorMesgEmpty,
			},
			auth.Auth{
				Enable:           enable,
				ClientId:         emptyString,
				ClientSecret:     validClientSecret,
				AuthorizeURL:     emptyString,
				TokenURL:         emptyString,
				GroupEndpointURL: emptyString,
				ScopeURL:         emptyString,
			},
		},
		{
			// Auth enabled, some configs are set to empty string
			[]string{
				errorMesgEmpty,
				errorMesgEmpty,
				errorMesgEmpty,
				errorMesgEmpty,
			},
			auth.Auth{
				Enable:           enable,
				ClientId:         validClientID,
				ClientSecret:     validClientSecret,
				AuthorizeURL:     emptyString,
				TokenURL:         emptyString,
				GroupEndpointURL: emptyString,
				ScopeURL:         emptyString,
			},
		},
		{
			// Auth enabled, some configs are set to empty string
			[]string{
				errorMesgEmpty,
				errorMesgEmpty,
				errorMesgEmpty,
			},
			auth.Auth{
				Enable:           enable,
				ClientId:         validClientID,
				ClientSecret:     validClientSecret,
				AuthorizeURL:     validTestURL,
				TokenURL:         emptyString,
				GroupEndpointURL: emptyString,
				ScopeURL:         emptyString,
			},
		},
		{
			// Auth enabled, some configs are set to empty string
			[]string{
				errorMesgEmpty,
				errorMesgEmpty,
			},
			auth.Auth{
				Enable:           enable,
				ClientId:         validClientID,
				ClientSecret:     validClientSecret,
				AuthorizeURL:     validTestURL,
				TokenURL:         validTestURL,
				GroupEndpointURL: emptyString,
				ScopeURL:         emptyString,
			},
		},
		{
			// Auth enabled, some strings are set to invalid URL
			[]string{
				errorMesgURL,
			},
			auth.Auth{
				Enable:           enable,
				ClientId:         validClientID,
				ClientSecret:     validClientSecret,
				AuthorizeURL:     notValidURL,
				TokenURL:         validTestURL,
				GroupEndpointURL: validTestURL,
				ScopeURL:         validTestURL,
			},
		},
		{
			// Auth enabled, some strings are set to invalid URL
			[]string{
				errorMesgURL,
				errorMesgURL,
			},
			auth.Auth{
				Enable:           enable,
				ClientId:         validClientID,
				ClientSecret:     validClientSecret,
				AuthorizeURL:     validTestURL,
				TokenURL:         notValidURL,
				GroupEndpointURL: notValidURL,
				ScopeURL:         validTestURL,
			},
		},
		{
			// Auth enabled, all configs are set properly
			[]string{},
			auth.Auth{
				Enable:           enable,
				ClientId:         validClientID,
				ClientSecret:     validClientSecret,
				AuthorizeURL:     validTestURL,
				TokenURL:         validTestURL,
				GroupEndpointURL: validTestURL,
				ScopeURL:         validTestURL,
			},
		},
		{
			// Auth disabled, all configs are set to empty string
			[]string{},
			auth.Auth{
				Enable:           disable,
				ClientId:         emptyString,
				ClientSecret:     emptyString,
				AuthorizeURL:     emptyString,
				TokenURL:         emptyString,
				GroupEndpointURL: emptyString,
				ScopeURL:         emptyString,
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			c := NewConfig()
			c.Auth = test.input

			errors, err := c.ValidateAuth()
			if len(test.expErrs) > 0 {
				if err == nil {
					t.Fatal("expected errors, but none were found")
				}
			}

			if len(errors) != len(test.expErrs) {
				fmt.Printf("%+v\n", errors)
				t.Fatalf("expected %v errors but got %v", len(test.expErrs), len(errors))
			}

			for i, e := range errors {
				if !strings.Contains(e.Error(), test.expErrs[i]) {
					t.Errorf("expected error to contain %s, but got %s", test.expErrs[i], e.Error())
				}
			}
		})
	}
}
