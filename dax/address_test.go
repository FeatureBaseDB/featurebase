package dax_test

import (
	"fmt"
	"testing"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/stretchr/testify/assert"
)

func TestAddress(t *testing.T) {
	t.Run("Address", func(t *testing.T) {
		tests := []struct {
			addr        dax.Address
			expScheme   string
			expHostPort string
			expHost     string
			expPort     uint16
			expPath     string
		}{
			{
				// blank address
				addr:        "",
				expScheme:   "",
				expHostPort: "",
				expHost:     "",
				expPort:     0,
			},
			{
				// schema://
				addr:        "http://",
				expScheme:   "http",
				expHostPort: "",
				expHost:     "",
				expPort:     0,
			},
			{
				// host
				addr:        "foo",
				expScheme:   "",
				expHostPort: "foo",
				expHost:     "foo",
				expPort:     0,
			},
			{
				// :port
				addr:        ":8080",
				expScheme:   "",
				expHostPort: ":8080",
				expHost:     "",
				expPort:     8080,
			},
			{
				// host:port
				addr:        "foo:8080",
				expScheme:   "",
				expHostPort: "foo:8080",
				expHost:     "foo",
				expPort:     8080,
			},
			{
				// schema://host:port
				addr:        "http://foo:8080",
				expScheme:   "http",
				expHostPort: "foo:8080",
				expHost:     "foo",
				expPort:     8080,
			},
			{
				// schema://host
				addr:        "http://foo",
				expScheme:   "http",
				expHostPort: "foo",
				expHost:     "foo",
				expPort:     0,
			},
			{
				// schema://:port
				addr:        "http://:8080",
				expScheme:   "http",
				expHostPort: ":8080",
				expHost:     "",
				expPort:     8080,
			},
			{
				// invalid port
				addr:        "http://foo:bar",
				expScheme:   "http",
				expHostPort: "foo",
				expHost:     "foo",
				expPort:     0,
			},
			{
				// :port outside of int16 range
				addr:        ":53308",
				expScheme:   "",
				expHostPort: ":53308",
				expHost:     "",
				expPort:     53308,
			},
			{
				// with path:
				addr:        "localhost:8080/foo/bar",
				expScheme:   "",
				expHostPort: "localhost:8080",
				expHost:     "localhost",
				expPort:     8080,
				expPath:     "foo/bar",
			},
		}

		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				assert.Equal(t, test.expScheme, test.addr.Scheme())
				assert.Equal(t, test.expHostPort, test.addr.HostPort())
				assert.Equal(t, test.expHost, test.addr.Host())
				assert.Equal(t, test.expPort, test.addr.Port())
				assert.Equal(t, test.expPath, test.addr.Path())
			})
		}
	})

	t.Run("OverrideScheme", func(t *testing.T) {
		tests := []struct {
			addr    dax.Address
			scheme  string
			expAddr string
		}{
			{
				addr:    "foo",
				scheme:  "http",
				expAddr: "http://foo",
			},
			{
				addr:    "http://foo",
				scheme:  "grpc",
				expAddr: "grpc://foo",
			},
			{
				addr:    "http://foo:8080",
				scheme:  "",
				expAddr: "foo:8080",
			},
			{
				addr:    "http://foo:8080/bar",
				scheme:  "",
				expAddr: "foo:8080/bar",
			},
		}

		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				assert.Equal(t, test.expAddr, test.addr.OverrideScheme(test.scheme))
			})
		}
	})

	t.Run("WithScheme", func(t *testing.T) {
		tests := []struct {
			addr    dax.Address
			scheme  string
			expAddr string
		}{
			{
				addr:    "foo",
				scheme:  "",
				expAddr: "://foo",
			},
			{
				addr:    "http://foo",
				scheme:  "grpc",
				expAddr: "http://foo",
			},
			{
				addr:    "http://foo:8080",
				scheme:  "",
				expAddr: "http://foo:8080",
			},
			{
				addr:    "foo:8080",
				scheme:  "grpc",
				expAddr: "grpc://foo:8080",
			},
			{
				addr:    "foo:8080/bar",
				scheme:  "grpc",
				expAddr: "grpc://foo:8080/bar",
			},
		}

		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				assert.Equal(t, test.expAddr, test.addr.WithScheme(test.scheme))
			})
		}
	})
}
