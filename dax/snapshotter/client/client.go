// Package client contains an http implementation of the WriteLogger client.
package client

// import (
// 	"bytes"
// 	"encoding/json"
// 	"fmt"
// 	"io"
// 	"net/http"
// 	"net/url"

// 	"github.com/molecula/featurebase/v3/dax"
// 	snapshotterhttp "github.com/molecula/featurebase/v3/dax/snapshotter/http"
// 	"github.com/molecula/featurebase/v3/errors"
// )

// const defaultScheme = "http"

// // TODO(jaffee): remove this?

// // Snapshotter is a client for the Snapshotter API methods.
// type Snapshotter struct {
// 	address dax.Address
// }

// func New(address dax.Address) *Snapshotter {
// 	return &Snapshotter{
// 		address: address,
// 	}
// }

// func (s *Snapshotter) Write(bucket string, key string, version int, rc io.ReadCloser) error {
// 	url := fmt.Sprintf("%s/snapshotter/write-snapshot?bucket=%s&key=%s&version=%d",
// 		s.address.WithScheme(defaultScheme),
// 		url.QueryEscape(bucket),
// 		url.QueryEscape(key),
// 		version,
// 	)

// 	// Post the request.
// 	resp, err := http.Post(url, "", rc)
// 	if err != nil {
// 		return errors.Wrap(err, "posting write-snapshot")
// 	}
// 	defer resp.Body.Close()

// 	if resp.StatusCode != http.StatusOK {
// 		b, _ := io.ReadAll(resp.Body)
// 		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
// 	}

// 	var wsr snapshotterhttp.WriteSnapshotResponse
// 	if err := json.NewDecoder(resp.Body).Decode(&wsr); err != nil {
// 		return errors.Wrap(err, "reading response body")
// 	}

// 	return nil
// }

// // WriteTo is exactly the same as Write, except that it takes an io.WriteTo
// // instead of an io.ReadCloser. This needs to be cleaned up so that we're only
// // using one or the other.
// func (s *Snapshotter) WriteTo(bucket string, key string, version int, wrTo io.WriterTo) error {
// 	url := fmt.Sprintf("%s/snapshotter/write-snapshot?bucket=%s&key=%s&version=%d",
// 		s.address.WithScheme(defaultScheme),
// 		url.QueryEscape(bucket),
// 		url.QueryEscape(key),
// 		version,
// 	)

// 	buf := &bytes.Buffer{}
// 	if _, err := wrTo.WriteTo(buf); err != nil {
// 		return errors.Wrap(err, "writing to buffer")
// 	}

// 	// Post the request.
// 	resp, err := http.Post(url, "", buf)
// 	if err != nil {
// 		return errors.Wrap(err, "posting write-snapshot")
// 	}
// 	defer resp.Body.Close()

// 	if resp.StatusCode != http.StatusOK {
// 		b, _ := io.ReadAll(resp.Body)
// 		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
// 	}

// 	var wsr snapshotterhttp.WriteSnapshotResponse
// 	if err := json.NewDecoder(resp.Body).Decode(&wsr); err != nil {
// 		return errors.Wrap(err, "reading response body")
// 	}

// 	return nil
// }

// func (s *Snapshotter) Read(bucket string, key string, version int) (io.ReadCloser, error) {
// 	url := fmt.Sprintf("%s/snapshotter/read-snapshot?bucket=%s&key=%s&version=%d",
// 		s.address.WithScheme(defaultScheme),
// 		url.QueryEscape(bucket),
// 		url.QueryEscape(key),
// 		version,
// 	)

// 	// Get the request.
// 	resp, err := http.Get(url)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "getting read-snapshot")
// 	}

// 	return resp.Body, nil
// }
