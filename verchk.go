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

type versionChecker struct {
	path   string
	url    string
	idfile string
}

func newVersionChecker(path, endpoint, idfile string) *versionChecker {
	v := versionChecker{
		path:   path,
		url:    endpoint,
		idfile: idfile,
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
	var filename string
	// if v.idfile starts with a path separator then it's an absolute path
	// otherwise it's a relative path and the file goes in the data directory
	if v.idfile[0] == os.PathSeparator {
		filename = v.idfile
	} else {
		filename = filepath.Join(v.path, v.idfile)
	}
	fh, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0744)
	if err != nil {
		return "", err
	}
	defer fh.Close()

	buf, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	// this just checks to see if there was anything at all in the file.
	// we should probably check to make sure it's a valid UUID
	if string(buf) == "" {
		id, err := v.generateClientUUID()
		if err != nil {
			return "", err
		}
		_, err = fh.WriteString(id)
		if err != nil {
			return "", err
		}
		return id, nil
	}

	return string(buf), nil
}

type verCheckResponse struct {
	Version string `json:"latest_version"`
	Error   string `json:"error"`
}
