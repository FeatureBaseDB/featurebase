package cli

import (
	"fmt"
	"io"
	"net/http"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/pkg/errors"
)

type Queryer interface {
	Query(org string, db string, sql io.Reader) (*featurebase.WireQueryResponse, error)
}

// Ensure type implements interface.
var _ Queryer = (*nopQueryer)(nil)

type nopQueryer struct{}

func (qryr *nopQueryer) Query(org string, db string, sql io.Reader) (*featurebase.WireQueryResponse, error) {
	return nil, errors.Errorf("no-op queryer")
}

// Ensure type implements interface.
var _ Queryer = (*standardQueryer)(nil)

// standardQueryer supports a standard featurebase deployment hitting the /sql
// endpoint with a payload containing only the sql statement.
type standardQueryer struct {
	Host string
	Port string
}

func (qryr *standardQueryer) Query(org string, db string, sql io.Reader) (*featurebase.WireQueryResponse, error) {
	url := fmt.Sprintf("%s/sql", hostPort(qryr.Host, qryr.Port))

	resp, err := http.Post(url, "application/json", sql)
	if err != nil {
		return nil, errors.Wrapf(err, "posting query")
	}

	fullbod, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading response")
	}

	sqlResponse := &featurebase.WireQueryResponse{}
	// TODO(tlt): switch this back once all responses are typed
	// TODO(twg) 2023/03/01 using json.Number to decode large ints so care must be made
	// if err := json.Unmarshal(fullbod, sqlResponse); err != nil {
	if err := sqlResponse.UnmarshalJSONTyped(fullbod, true); err != nil {
		return nil, errors.Wrapf(err, "unmarshaling query response, body:\n'%s'\n", fullbod)
	}

	return sqlResponse, nil
}

// Ensure type implements interface.
var _ Queryer = (*serverlessQueryer)(nil)

// serverlessQueryer is similar to the standardQueryer except that it hits a
// different endpoint, and its payload is database-aware.
type serverlessQueryer struct {
	Host string
	Port string
}

func (qryr *serverlessQueryer) Query(org string, db string, sql io.Reader) (*featurebase.WireQueryResponse, error) {
	if org == "" {
		return nil, NewErrOrganizationRequired()
	}

	url := fmt.Sprintf("%s/queryer/databases/%s/sql", hostPort(qryr.Host, qryr.Port), db)
	if db == "" {
		url = fmt.Sprintf("%s/queryer/sql", hostPort(qryr.Host, qryr.Port))
	}

	client := &http.Client{
		Timeout: time.Second * 30,
	}

	req, err := http.NewRequest(http.MethodPost, url, sql)
	if err != nil {
		return nil, errors.Wrap(err, "creating new post request")
	}
	req.Header.Add("Content-Type", "text/plain")
	req.Header.Add("OrganizationID", org)

	var resp *http.Response
	if resp, err = client.Do(req); err != nil {
		return nil, errors.Wrap(err, "executing post request")
	}

	fullbod, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading response")
	}
	sqlResponse := &featurebase.WireQueryResponse{}
	// TODO(tlt): switch this back once all responses are typed
	// if err := json.Unmarshal(fullbod, sqlResponse); err != nil {
	if err := sqlResponse.UnmarshalJSONTyped(fullbod, true); err != nil {
		return nil, errors.Wrapf(err, "unmarshaling query response, body:\n'%s'\n", fullbod)
	}

	return sqlResponse, nil
}
