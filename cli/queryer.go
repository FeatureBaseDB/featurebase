package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	featurebase "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/dax"
	queryerhttp "github.com/molecula/featurebase/v3/dax/queryer/http"
	"github.com/pkg/errors"
)

type Queryer interface {
	Query(org, db, sql string) (*featurebase.WireQueryResponse, error)
}

// Ensure type implements interface.
var _ Queryer = (*standardQueryer)(nil)

// standardQueryer supports a standard featurebase deployment hitting the /sql
// endpoint with a payload containing only the sql statement.
type standardQueryer struct {
	Host string
	Port string
}

func (qryr *standardQueryer) Query(org, db, sql string) (*featurebase.WireQueryResponse, error) {
	buf := bytes.Buffer{}
	url := fmt.Sprintf("%s/sql", hostPort(qryr.Host, qryr.Port))

	buf.Write([]byte(sql))

	resp, err := http.Post(url, "application/json", &buf)
	if err != nil {
		return nil, errors.Wrapf(err, "posting query")
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

// Ensure type implements interface.
var _ Queryer = (*daxQueryer)(nil)

// daxQueryer is similar to the standardQueryer except that it hits a different
// endpoint, and its payload is a json object which includes, in addition to the
// sql statement, things like org and db.
type daxQueryer struct {
	Host string
	Port string
}

func (qryr *daxQueryer) Query(org, db, sql string) (*featurebase.WireQueryResponse, error) {
	buf := bytes.Buffer{}
	url := fmt.Sprintf("%s/queryer/sql", hostPort(qryr.Host, qryr.Port))

	sqlReq := &queryerhttp.SQLRequest{
		OrganizationID: dax.OrganizationID(org),
		DatabaseID:     dax.DatabaseID(db),
		SQL:            sql,
	}
	if err := json.NewEncoder(&buf).Encode(sqlReq); err != nil {
		return nil, errors.Wrapf(err, "encoding sql request: %s", sql)
	}

	resp, err := http.Post(url, "application/json", &buf)
	if err != nil {
		return nil, errors.Wrapf(err, "posting query")
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
