// Package client contains an http implementation of the WriteLogger client.
package client

// import (
// 	"bytes"
// 	"encoding/json"
// 	"fmt"
// 	"io"
// 	"net/http"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
)

// // TODO(jaffee): remove this?

// const defaultScheme = "http"

// // WriteLogger is a client for the WriteLogger API methods.
// type WriteLogger struct {
// 	address dax.Address
// }

// func New(address dax.Address) *WriteLogger {
// 	return &WriteLogger{
// 		address: address,
// 	}
// }

// func (w *WriteLogger) AppendMessage(bucket string, key string, version int, msg []byte) error {
// 	url := fmt.Sprintf("%s/writelogger/append-message", w.address.WithScheme(defaultScheme))

// 	req := &AppendMessageRequest{
// 		Bucket:  bucket,
// 		Key:     key,
// 		Version: version,
// 		Message: msg,
// 	}

// 	// Encode the request.
// 	postBody, err := json.Marshal(req)
// 	if err != nil {
// 		return errors.Wrap(err, "marshalling post request")
// 	}
// 	requestBody := bytes.NewBuffer(postBody)

// 	// Post the request.
// 	resp, err := http.Post(url, "application/json", requestBody)
// 	if err != nil {
// 		return errors.Wrap(err, "posting append-message request")
// 	}
// 	defer resp.Body.Close()

// 	if resp.StatusCode != http.StatusOK {
// 		b, _ := io.ReadAll(resp.Body)
// 		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
// 	}

// 	var isr *AppendMessageResponse
// 	if err := json.NewDecoder(resp.Body).Decode(&isr); err != nil {
// 		return errors.Wrap(err, "reading response body")
// 	}

// 	return nil
// }

// type AppendMessageRequest struct {
// 	Bucket  string `json:"bucket"`
// 	Key     string `json:"key"`
// 	Version int    `json:"version"`
// 	Message []byte `json:"message"`
// }
// type AppendMessageResponse struct{}

// func (w *WriteLogger) LogReader(bucket string, key string, version int) (io.Reader, io.Closer, error) {
// 	url := fmt.Sprintf("%s/writelogger/log-reader", w.address.WithScheme(defaultScheme))

// 	req := &LogReaderRequest{
// 		Bucket:  bucket,
// 		Version: version,
// 		Key:     key,
// 	}

// 	// Encode the request.
// 	postBody, err := json.Marshal(req)
// 	if err != nil {
// 		return nil, nil, errors.Wrap(err, "marshalling post request")
// 	}
// 	requestBody := bytes.NewBuffer(postBody)

// 	// Post the request.
// 	resp, err := http.Post(url, "application/json", requestBody)
// 	if err != nil {
// 		return nil, nil, errors.Wrap(err, "posting log-reader request")
// 	}

// 	if resp.StatusCode != http.StatusOK {
// 		b, _ := io.ReadAll(resp.Body)
// 		defer resp.Body.Close()
// 		return nil, nil, errors.Errorf("status code: %d: %s", resp.StatusCode, b)
// 	}

// 	return resp.Body, resp.Body, nil
// }

// type LogReaderRequest struct {
// 	Bucket  string `json:"bucket"`
// 	Version int    `json:"version"`
// 	Key     string `json:"key"`
// }

// func (w *WriteLogger) DeleteLog(bucket string, key string, version int) error {
// 	url := fmt.Sprintf("%s/writelogger/delete-log", w.address.WithScheme(defaultScheme))

// 	req := &DeleteLogRequest{
// 		Bucket:  bucket,
// 		Version: version,
// 		Key:     key,
// 	}

// 	// Encode the request.
// 	postBody, err := json.Marshal(req)
// 	if err != nil {
// 		return errors.Wrap(err, "marshalling post request")
// 	}
// 	requestBody := bytes.NewBuffer(postBody)

// 	// Post the request.
// 	resp, err := http.Post(url, "application/json", requestBody)
// 	if err != nil {
// 		return errors.Wrap(err, "posting log-reader request")
// 	}

// 	if resp.StatusCode != http.StatusOK {
// 		b, _ := io.ReadAll(resp.Body)
// 		defer resp.Body.Close()
// 		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
// 	}

// 	return nil
// }

// type DeleteLogRequest struct {
// 	Bucket  string `json:"bucket"`
// 	Version int    `json:"version"`
// 	Key     string `json:"key"`
// }
