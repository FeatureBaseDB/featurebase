package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/pilosa/pilosa"
)

// Ensure implementation implements inteface.
var _ pilosa.TranslateStore = (*translateStore)(nil)

// translateStore represents an implementation of translateStore that
// communicates over HTTP. This is used with the TranslateHandler.
type translateStore struct {
	URL string
}

// NewTranslateStore returns a new instance of TranslateStore.
func NewTranslateStore(rawurl string) *translateStore {
	return &translateStore{URL: rawurl}
}

// TranslateColumnsToUint64 is not currently implemented.
func (s *translateStore) TranslateColumnsToUint64(index string, values []string) ([]uint64, error) {
	return nil, pilosa.ErrNotImplemented
}

// TranslateColumnToString is not currently implemented.
func (s *translateStore) TranslateColumnToString(index string, values uint64) (string, error) {
	return "", pilosa.ErrNotImplemented
}

// TranslateRowsToUint64 is not currently implemented.
func (s *translateStore) TranslateRowsToUint64(index, frame string, values []string) ([]uint64, error) {
	return nil, pilosa.ErrNotImplemented
}

// TranslateRowToString is not currently implemented.
func (s *translateStore) TranslateRowToString(index, frame string, values uint64) (string, error) {
	return "", pilosa.ErrNotImplemented
}

// Reader returns a reader that can stream data from a remote store.
func (s *translateStore) Reader(ctx context.Context, off int64) (io.ReadCloser, error) {
	// Generate remote URL.
	u, err := url.Parse(s.URL)
	if err != nil {
		return nil, err
	}
	u.Path = "/internal/translate/data"
	u.RawQuery = (url.Values{
		"offset": {strconv.FormatInt(off, 10)},
	}).Encode()

	// Connect a stream to the remote server.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	// Connect a stream to the remote server.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http: cannot connect to translate store endpoint: %s", err)
	}

	// Handle error codes or return body as stream.
	switch resp.StatusCode {
	case http.StatusOK:
		return resp.Body, nil
	case http.StatusNotImplemented:
		resp.Body.Close()
		return nil, pilosa.ErrNotImplemented
	default:
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("http: invalid translate store endpoint status: code=%d url=%s body=%q", resp.StatusCode, u.String(), bytes.TrimSpace(body))
	}
}
