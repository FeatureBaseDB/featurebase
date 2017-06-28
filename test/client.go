package test

import (
	"github.com/pilosa/pilosa"
)

// Client represents a test wrapper for pilosa.Client.
type Client struct {
	*pilosa.Client
}

// MustNewClient returns a new instance of Client. Panic on error.
func MustNewClient(host string) *Client {
	c, err := pilosa.NewClient(host)
	if err != nil {
		panic(err)
	}
	return &Client{Client: c}
}
