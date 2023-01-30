package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	queryerhttp "github.com/featurebasedb/featurebase/v3/dax/queryer/http"
	"github.com/pkg/errors"
)

type Queryer interface {
	Query(org string, db string, sql io.Reader) (*featurebase.WireQueryResponse, error)
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

func (qryr *daxQueryer) Query(org string, db string, sql io.Reader) (*featurebase.WireQueryResponse, error) {
	buf := bytes.Buffer{}
	url := fmt.Sprintf("%s/queryer/sql", hostPort(qryr.Host, qryr.Port))

	// TODO(tlt): this needs to be removed; we should be passing the sql
	// io.Reader to the http.Post() body. In order to do that, we need to get
	// rid of this json object.
	tmpBuf := new(strings.Builder)
	_, err := io.Copy(tmpBuf, sql)
	if err != nil {
		return nil, err
	}

	sqlReq := &queryerhttp.SQLRequest{
		OrganizationID: dax.OrganizationID(org),
		DatabaseID:     dax.DatabaseID(db),
		SQL:            tmpBuf.String(),
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
