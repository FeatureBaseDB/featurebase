// Copyright 2021 Molecula Corp. All rights reserved.
package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
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
	errorMesgEmpty := "empty string"
	errorMesgURL := "invalid URL"
	errorMesgScope := "must provide scope"
	errorMesgKey := "invalid key length"
	validTestURL := "https://url.com/"
	validClientID := "clientid"
	validClientSecret := "clientSecret"
	validKey := "3db6665be8b860af422155acf2346d4fcb46678fca42e60d934abe0b7ce43600"
	invalidURL := "not-a-url"
	emptyString := ""
	validStringSlice := []string{"https://graph.microsoft.com/.default", "offline_access"}
	validString := "asdfqwer1234asdfzxcv"
	var emptySlice []string

	enable := true
	disable := false

	tests := []struct {
		expErrs []string
		input   Auth
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
				errorMesgEmpty,
				errorMesgEmpty,
			},
			Auth{
				Enable:           enable,
				ClientId:         emptyString,
				ClientSecret:     emptyString,
				AuthorizeURL:     emptyString,
				RedirectBaseURL:  emptyString,
				TokenURL:         emptyString,
				GroupEndpointURL: emptyString,
				LogoutURL:        emptyString,
				Scopes:           validStringSlice,
				SecretKey:        emptyString,
			},
		},
		{
			// Auth enabled, keys are invalid length
			[]string{
				errorMesgKey,
			},
			Auth{
				Enable:           enable,
				ClientId:         validClientID,
				ClientSecret:     validClientSecret,
				AuthorizeURL:     validTestURL,
				TokenURL:         validTestURL,
				RedirectBaseURL:  validTestURL,
				GroupEndpointURL: validTestURL,
				LogoutURL:        validTestURL,
				Scopes:           validStringSlice,
				SecretKey:        validString,
			},
		},
		{
			// Auth enabled, some URLs are set to invalid URL
			[]string{
				errorMesgURL,
				errorMesgURL,
				errorMesgURL,
			},
			Auth{
				Enable:           enable,
				ClientId:         validClientID,
				ClientSecret:     validClientSecret,
				AuthorizeURL:     validTestURL,
				TokenURL:         invalidURL,
				GroupEndpointURL: invalidURL,
				RedirectBaseURL:  validTestURL,
				LogoutURL:        invalidURL,
				Scopes:           validStringSlice,
				SecretKey:        validKey,
			},
		},
		{
			// Auth enabled, all configs are set properly except scope
			[]string{
				errorMesgScope,
			},
			Auth{
				Enable:           enable,
				ClientId:         validClientID,
				ClientSecret:     validClientSecret,
				AuthorizeURL:     validTestURL,
				TokenURL:         validTestURL,
				GroupEndpointURL: validTestURL,
				RedirectBaseURL:  validTestURL,
				LogoutURL:        validTestURL,
				Scopes:           emptySlice,
				SecretKey:        validKey,
			},
		},
		{
			// Auth enabled, all configs are set properly
			[]string{},
			Auth{
				Enable:           enable,
				ClientId:         validClientID,
				ClientSecret:     validClientSecret,
				AuthorizeURL:     validTestURL,
				TokenURL:         validTestURL,
				RedirectBaseURL:  validTestURL,
				GroupEndpointURL: validTestURL,
				LogoutURL:        validTestURL,
				Scopes:           validStringSlice,
				SecretKey:        validKey,
				QueryLogPath:     "thisIsAPAth",
			},
		},
		{
			// Auth disabled, some configs are set to values
			[]string{},
			Auth{
				Enable:           disable,
				ClientId:         emptyString,
				ClientSecret:     validString,
				AuthorizeURL:     emptyString,
				RedirectBaseURL:  validTestURL,
				TokenURL:         emptyString,
				GroupEndpointURL: invalidURL,
				LogoutURL:        validTestURL,
				Scopes:           validStringSlice,
				SecretKey:        emptyString,
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			c := NewConfig()
			c.Auth = test.input

			errors := c.ValidateAuth()
			if len(test.expErrs) > 0 {
				if errors == nil {
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

func TestConfig_validatePermissions(t *testing.T) {
	permissions0 := ``

	permissions1 := `user-groups:
  "":
    "test": "read"
admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe"`

	permissions2 := `user-groups:
  "dca35310-ecda-4f23-86cd-876aee559900":
    "": "write"
admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe"`

	permissions3 := `user-groups:
  "dca35310-ecda-4f23-86cd-876aee559900":
    "test": ""
admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe"`

	permissions4 := `user-groups:
  "dca35310-ecda-4f23-86cd-876aee559900":
    "test": "readwrite"
admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe"`

	permissions5 := `user-groups:
  "dca35310-ecda-4f23-86cd-876aee559900":
    "test": "read"`

	tests := []struct {
		err   string
		input string
	}{
		{
			"no group permissions found in permissions file",
			permissions0,
		},
		{
			"empty string for group id",
			permissions1,
		},
		{
			"empty string for index",
			permissions2,
		},
		{
			"empty string for permission",
			permissions3,
		},
		{
			"not a valid permission",
			permissions4,
		},
		{
			"empty string for admin in permissions file",
			permissions5,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {

			c := NewConfig()
			c.Auth.PermissionsFile = "test.yaml"

			permFile := strings.NewReader(test.input)
			errors := c.ValidatePermissions(permFile)

			if errors == nil {
				t.Fatal("expected errors, but none were found")
			}

			for _, err := range errors {
				if !strings.Contains(err.Error(), test.err) {
					t.Errorf("expected error to contain %s, but got %s", test.err, err.Error())

				}
			}
		})
	}
}

func TestConfig_validatePermissionsFilename(t *testing.T) {

	tests := []struct {
		err   string
		input string
	}{
		{
			"empty string for auth config permissions file",
			"",
		},
		{
			"invalid file extension for auth config permissions file",
			"permissions.txt",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			c := NewConfig()
			c.Auth.PermissionsFile = test.input

			if err := c.ValidatePermissionsFile(); err != nil {
				if !strings.Contains(err.Error(), test.err) {
					t.Errorf("expected error to contain %s, but got %s", test.err, err.Error())
				}
			}
		})
	}
}
