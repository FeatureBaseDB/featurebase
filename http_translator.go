// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"sync"

	"github.com/molecula/featurebase/v3/logger"
)

func GetOpenTranslateReaderFunc(client *http.Client) OpenTranslateReaderFunc {
	return GetOpenTranslateReaderWithLockerFunc(client, nopLocker{})
}

func GetOpenTranslateReaderWithLockerFunc(client *http.Client, locker sync.Locker) OpenTranslateReaderFunc {
	lockType := reflect.TypeOf(locker)
	if lockType.Kind() == reflect.Ptr {
		lockType = lockType.Elem()
	}
	return func(ctx context.Context, nodeURL string, offsets TranslateOffsetMap) (TranslateEntryReader, error) {
		return openTranslateReader(ctx, nodeURL, offsets, client, reflect.New(lockType).Interface().(sync.Locker))
	}
}

func openTranslateReader(ctx context.Context, nodeURL string, offsets TranslateOffsetMap, client *http.Client, locker sync.Locker) (TranslateEntryReader, error) {
	r := NewTranslateEntryReader(ctx, client)
	r.locker = locker

	r.URL = nodeURL + "/internal/translate/data"
	r.Offsets = offsets
	if err := r.Open(); err != nil {
		return nil, err
	}
	return r, nil
}

type nopLocker struct{}

func (nopLocker) Lock()   {}
func (nopLocker) Unlock() {}

// TranslateEntryReader represents an implementation of TranslateEntryReader.
// It consolidates all index & field translate entries into a single reader.
type HTTPTranslateEntryReader struct {
	locker sync.Locker

	ctx    context.Context
	cancel func()

	body io.ReadCloser
	dec  *json.Decoder

	// Lookup of offsets for each index & field.
	// Must be set before calling Open().
	Offsets TranslateOffsetMap

	// URL to stream entries from.
	// Must be set before calling Open().
	URL string

	HTTPClient *http.Client

	Logger logger.Logger
}

// NewTranslateEntryReader returns a new instance of TranslateEntryReader.
func NewTranslateEntryReader(ctx context.Context, client *http.Client) *HTTPTranslateEntryReader {
	if client == nil {
		client = http.DefaultClient
	}
	r := &HTTPTranslateEntryReader{locker: nopLocker{}, HTTPClient: client, Logger: logger.NopLogger}
	r.ctx, r.cancel = context.WithCancel(ctx)
	return r
}

// Open initiates the reader.
func (r *HTTPTranslateEntryReader) Open() error {
	// Serialize map of offsets to request body.
	requestBody, err := json.Marshal(r.Offsets)
	if err != nil {
		return err
	}

	// Connect a stream to the remote server.
	req, err := http.NewRequest("POST", r.URL, bytes.NewReader(requestBody))
	if err != nil {
		return err
	}
	req = req.WithContext(r.ctx)

	// Connect a stream to the remote server.
	resp, err := r.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("http: cannot connect to translate store endpoint: url=%s err=%s", r.URL, err)
	}
	r.body = resp.Body
	r.dec = json.NewDecoder(r.body)

	// Handle error codes.
	if resp.StatusCode == http.StatusNotImplemented {
		r.body.Close()
		return ErrNotImplemented
	} else if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		r.body.Close()
		return fmt.Errorf("http: invalid translate store endpoint status: code=%d url=%s body=%q", resp.StatusCode, r.URL, bytes.TrimSpace(body))
	}
	return nil
}

// Close stops the reader.
func (r *HTTPTranslateEntryReader) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	if r.body != nil {
		r.locker.Lock()
		err := r.body.Close()
		r.locker.Unlock()
		return err
	}
	return nil
}

// ReadEntry reads the next entry from the stream into entry.
// Returns io.EOF at the end of the stream.
func (r *HTTPTranslateEntryReader) ReadEntry(entry *TranslateEntry) error {
	r.locker.Lock()
	defer r.locker.Unlock()

	return r.dec.Decode(&entry)
}
