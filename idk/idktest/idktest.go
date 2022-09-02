package idktest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/pkg/errors"
)

const (
	ErrDeletingIndex = "deleting index"
	ErrRunningIngest = "running ingester"
	ErrGettingSchema = "getting schema"
	ErrGettingQuery  = "querying"
)

type ExtractResponse struct {
	Results []Result
}

type Result struct {
	Fields  []erField
	Columns []Column
}

type erField struct {
	Name string
	Type string
}

type Column struct {
	ColumnID int           `json:"column"`
	Rows     []interface{} `json:"rows"`
}

// doExtractQuery sends a query of result-type Extract to pilosa and
// unpacks the response.
// This was created as a helper function for TestLookupFieldWithExternalId.
func DoExtractQuery(pql, index string) (ExtractResponse, error) {
	pilosaHost, ok := os.LookupEnv("IDK_TEST_PILOSA_HOST")
	if !ok {
		pilosaHost = "pilosa:10101"
	}

	var eResp ExtractResponse

	body := strings.NewReader(pql)
	url := fmt.Sprintf("http://%s/index/%s/query", pilosaHost, index)
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return eResp, errors.Errorf("creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return eResp, errors.Errorf("sending request: %v", err)
	}
	defer resp.Body.Close()

	s, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return eResp, errors.Errorf("reading response: %v", err)
	}

	err = json.Unmarshal(s, &eResp)
	if err != nil {
		return eResp, errors.Errorf("unmarshaling response: %v", err)
	}

	return eResp, nil
}
