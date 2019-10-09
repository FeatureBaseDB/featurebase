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

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/logger"
)

func OpenTranslateReader(ctx context.Context, nodeURL string, offsets pilosa.TranslateOffsetMap) (pilosa.TranslateEntryReader, error) {
	r := NewTranslateEntryReader(ctx)
	r.URL = nodeURL + "/internal/translate/data"
	r.Offsets = offsets
	if err := r.Open(); err != nil {
		return nil, err
	}
	return r, nil
}

// TranslateEntryReader represents an implementation of pilosa.TranslateEntryReader.
// It consolidates all index & field translate entries into a single reader.
type TranslateEntryReader struct {
	ctx    context.Context
	cancel func()

	body io.ReadCloser
	dec  *json.Decoder

	// Lookup of offsets for each index & field.
	// Must be set before calling Open().
	Offsets pilosa.TranslateOffsetMap

	// URL to stream entries from.
	// Must be set before calling Open().
	URL string

	HTTPClient *http.Client

	Logger logger.Logger
}

// NewTranslateEntryReader returns a new instance of TranslateEntryReader.
func NewTranslateEntryReader(ctx context.Context) *TranslateEntryReader {
	r := &TranslateEntryReader{HTTPClient: http.DefaultClient, Logger: logger.NopLogger}
	r.ctx, r.cancel = context.WithCancel(ctx)
	return r
}

// Open initiates the reader.
func (r *TranslateEntryReader) Open() error {
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
		return pilosa.ErrNotImplemented
	} else if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		r.body.Close()
		return fmt.Errorf("http: invalid translate store endpoint status: code=%d url=%s body=%q", resp.StatusCode, r.URL, bytes.TrimSpace(body))
	}
	return nil
}

// Close stops the reader.
func (r *TranslateEntryReader) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	if r.body != nil {
		return r.body.Close()
	}
	return nil
}

// ReadEntry reads the next entry from the stream into entry.
// Returns io.EOF at the end of the stream.
func (r *TranslateEntryReader) ReadEntry(entry *pilosa.TranslateEntry) error {
	return r.dec.Decode(&entry)
}
