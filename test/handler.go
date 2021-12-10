// Copyright 2021 Molecula Corp. All rights reserved.
package test

import (
	"encoding/json"
	"io"
	gohttp "net/http"
)

// MustNewHTTPRequest creates a new HTTP request. Panic on error.
func MustNewHTTPRequest(method, urlStr string, body io.Reader) *gohttp.Request {
	req, err := gohttp.NewRequest(method, urlStr, body)
	req.Header.Add("Accept", "application/json")
	if err != nil {
		panic(err)
	}
	return req
}

// MustMarshalJSON marshals v to JSON. Panic on error.
func MustMarshalJSON(v interface{}) []byte {
	buf, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return buf
}
