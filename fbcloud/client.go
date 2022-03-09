package fbcloud

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	featurebase "github.com/molecula/featurebase/v3"
	"github.com/pkg/errors"
)

// TokenRefreshTimeout is currently hardcoded to be just under the
// Cognito token timeout for cloud which is 15 minutes (I think I
// heard that somewhere anyway). It seems to work.
const TokenRefreshTimeout = time.Minute * 13

type Queryer struct {
	Host string

	ClientID string
	Region   string
	Email    string
	Password string

	token       string
	lastRefresh time.Time
}

func (cq *Queryer) tokenRefresh() error {
	token, err := authenticate(cq.ClientID, cq.Region, cq.Email, cq.Password)
	if err != nil {
		return errors.Wrap(err, "getting token")
	}
	cq.token = token
	cq.lastRefresh = time.Now()
	fmt.Println("refreshed auth token")
	return nil
}

type tokenizedSQL struct {
	Language  string `json:"language"`
	Statement string `json:"statement"`
}

// Query issues a SQL query formatted for the FeatureBase cloud query endpoint.
func (cq *Queryer) Query(org, db, sql string) (*featurebase.SQLResponse, error) {
	if time.Since(cq.lastRefresh) > TokenRefreshTimeout {
		if err := cq.tokenRefresh(); err != nil {
			return nil, errors.Wrap(err, "refreshing token")
		}
	}
	url := fmt.Sprintf("%s/v2/databases/%s/query", cq.Host, db)

	sqlReq := &tokenizedSQL{
		Language:  "sql",
		Statement: sql,
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(sqlReq); err != nil {
		return nil, errors.Wrapf(err, "encoding sql request: %s", sql)
	}

	client := &http.Client{
		Timeout: time.Second * 30,
	}
	req, err := http.NewRequest(http.MethodPost, url, &buf)
	if err != nil {
		return nil, errors.Wrap(err, "creating new post request")
	}
	req.Header.Add("Authorization", cq.token)

	var resp *http.Response
	if resp, err = client.Do(req); err != nil {
		return nil, errors.Wrap(err, "executing post request")
	}

	fullbod, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading cloud response")
	}
	if resp.StatusCode/100 != 2 {
		return nil, errors.Errorf("unexpected status: %s, full body: '%s'", resp.Status, fullbod)
	}
	var cloudResp cloudResponse
	if err := json.Unmarshal(fullbod, &cloudResp); err != nil {
		return nil, errors.Wrapf(err, "decoding cloud response, body:\n%s", fullbod)
	}
	sqlResponse := cloudResp.Results
	return &sqlResponse, nil
}

// HTTPRequest can make an arbitrary http request to the host and
// tries to json unmarshal the response body into v if v is
// non-nil. This is handy for hitting cloud endpoints other than the
// query endpoint which is handled by Query. I don't think this is
// currently used, but I'd like to keep it around for debugging.
func (cq *Queryer) HTTPRequest(method, path, body string, v interface{}) ([]byte, error) {
	if time.Since(cq.lastRefresh) > TokenRefreshTimeout {
		if err := cq.tokenRefresh(); err != nil {
			return nil, errors.Wrap(err, "refreshing token")
		}
	}
	var bod io.Reader
	if body == "" {
		bod = nil
	} else {
		bod = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, fmt.Sprintf("%s%s", cq.Host, path), bod)
	if err != nil {
		return nil, errors.Errorf("creating request: %v", err)
	}
	// fmt.Printf("%+v\n", req)

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", cq.token))
	if bod != nil {
		req.Header.Add("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Errorf("doing request: %v", err)
	}
	bodbytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Errorf("reading response body: %v", err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, errors.Errorf("bad status: %s. body: '%s'", resp.Status, bodbytes)
	}

	if v != nil {
		err = json.Unmarshal(bodbytes, v)
		if err != nil {
			return nil, errors.Errorf("unmarshaling: %v", err)
		}
	}

	return bodbytes, nil
}

type cloudResponse struct {
	Results featurebase.SQLResponse `json:"results"`
}
