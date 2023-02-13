package pilosa

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

const (
	versionCheckerFilename = ".client_id.txt"
)

type versionChecker struct {
	path string
	url  string
}

func newVersionChecker(path, endpoint string) *versionChecker {
	v := versionChecker{
		path: path,
		url:  endpoint,
	}
	return &v
}

func (v *versionChecker) checkIn() (*verCheckResponse, error) {
	id, err := v.writeClientUUID()
	if err != nil {
		return nil, err
	}

	body := make(map[string]string, 2)
	body["entry_type"] = "user"
	body["version"] = Version
	body["client_id"] = id

	req, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	wReq := bytes.NewReader(req)

	var jsonResp verCheckResponse
	r, err := http.Post(v.url, "application/json", wReq)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &jsonResp)
	if err != nil {

		return nil, err
	}

	return &jsonResp, nil
}

func (v *versionChecker) generateClientUUID() (string, error) {
	clientUUID := uuid.New()
	cleanedUUID := strings.Replace(clientUUID.String(), "-", "", -1)
	return cleanedUUID, nil
}

func (v *versionChecker) writeClientUUID() (string, error) {
	filename := filepath.Join(v.path, versionCheckerFilename)
	if _, err := os.Stat(filename); err != nil {
		if os.IsNotExist(err) {
			fh, err := os.Create(filename)
			if err != nil {
				return "", err
			}
			defer fh.Close()

			id, err := v.generateClientUUID()
			if err != nil {
				return "", err
			}

			if _, err := fh.WriteString(id); err != nil {
				return "", err
			}

			return id, nil
		} else {
			return "", err
		}
	}

	fh, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer fh.Close()

	buf, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

type verCheckResponse struct {
	Version string `json:"latest_version"`
}
