package pilosa

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
)

type VersionChecker struct {
	URL string
}

func NewVersionChecker(endpoint string) *VersionChecker {
	v := VersionChecker{
		URL: endpoint,
	}
	return &v
}

func (v *VersionChecker) CheckIn() (*Response, error) {

	body := make(map[string]string, 2)
	body["entry_type"] = "user"
	body["version"] = VersionInfo(true)
	req, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	_, err = http.Post(v.URL, "application/json", bytes.NewBuffer(req))
	if err != nil {
		return nil, err
	}

	var json_resp Response
	r, err := http.Get(v.URL)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &json_resp)
	if err != nil {

		return nil, err
	}
	return &json_resp, nil

}

type Response struct {
	Information InfoSubResponse `json:"info"`
}

type InfoSubResponse struct {
	Version string `json:"version"`
}
