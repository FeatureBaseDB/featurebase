package pilosa

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/google/uuid"
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

func (v *VersionChecker) CheckIn() (*VerCheckResponse, error) {

	id, err := v.WriteClientUUID()
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

	if err != nil {
		return nil, err
	}

	var json_resp VerCheckResponse
	r, err := http.Post(v.URL, "application/json", wReq)
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

func (v *VersionChecker) GenerateClientUUID() (string, error) {
	clientUUID := uuid.New()
	cleanedUUID := strings.Replace(clientUUID.String(), "-", "", -1)
	return cleanedUUID, nil
}

func (v *VersionChecker) WriteClientUUID() (string, error) {
	filename := ".client_id.txt"
	_, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			fh, err := os.Create(filename)
			if err != nil {
				return "", err
			}
			defer fh.Close()
			id, err := v.GenerateClientUUID()
			if err != nil {
				return "", err
			}

			_, err = fh.WriteString(id)
			if err != nil {
				return "", err
			}

			return "", err
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

type VerCheckResponse struct {
	Version string `json:"latest_version"`
}
