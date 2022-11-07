package csrc

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/pkg/errors"
)

type Client struct {
	URL string

	httpClient *http.Client
	Auth       *BasicAuth
}
type BasicAuth struct {
	KafkaSchemaApiKey    string
	KafkaSchemaApiSecret string
}

func NewClient(url string, tlsConfig *tls.Config, authConfig *BasicAuth) *Client {
	if !strings.HasPrefix(url, "http") {
		url = "http://" + url
	}
	c := http.DefaultClient
	if strings.HasPrefix(url, "https://") {
		log.Printf("getting http client with tls config: %v", tlsConfig)
		c = idk.GetHTTPClient(tlsConfig)
	}
	return &Client{
		URL: url,

		httpClient: c,
		Auth:       authConfig,
	}
}

// GetSchema gets the schema with the ID.
// https://docs.confluent.io/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id
func (c *Client) GetSchema(id int) (string, error) {
	//	schemaUrlResponse, err := s.httpClient.Get(schemaUrl)
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/schemas/ids/%d", c.URL, id), http.NoBody)
	if err != nil {
		return "", errors.Wrap(err, "building request for getting schema from registry")
	}
	if c.Auth != nil {
		req.SetBasicAuth(c.Auth.KafkaSchemaApiKey, c.Auth.KafkaSchemaApiSecret)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "getting schema from registry")
	}
	defer resp.Body.Close()
	sr := SchemaResponse{}
	err = unmarshalRespErr(resp, err, &sr)
	if err != nil {
		return "", errors.Wrap(err, "making http request")
	}
	return sr.Schema, nil
}

type SchemaResponse struct {
	Schema  string `json:"schema"`  // The actual AVRO schema
	Subject string `json:"subject"` // Subject where the schema is registered for
	Version int    `json:"version"` // Version within this subject
	ID      int    `json:"id"`      // Registry's unique id
}

type ErrorResponse struct {
	StatusCode int    `json:"error_code"`
	Body       string `json:"message"`
}

func (e *ErrorResponse) Error() string {
	return fmt.Sprintf("status %d: %s", e.StatusCode, e.Body)
}

func (c *Client) PostSubjects(subj, schema string) (*SchemaResponse, error) {
	schema = strings.Replace(schema, "\t", "", -1)
	schema = strings.Replace(schema, "\n", `\n`, -1)
	schema = fmt.Sprintf(`{"schema": "%s"}`, strings.Replace(schema, `"`, `\"`, -1)) // this is probably terrible

	//	schemaUrlResponse, err := s.httpClient.Get(schemaUrl)
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/subjects/%s/versions", c.URL, subj), strings.NewReader(schema))
	if err != nil {
		return nil, errors.Wrap(err, "building request to post subjects")
	}
	if c.Auth != nil {
		req.SetBasicAuth(c.Auth.KafkaSchemaApiKey, c.Auth.KafkaSchemaApiSecret)
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "posting subjects")
	}
	defer resp.Body.Close()
	sr := &SchemaResponse{}
	err = unmarshalRespErr(resp, err, sr)
	if err != nil {
		return nil, errors.Wrapf(err, "unmarshaling resp to %s", fmt.Sprintf("%s/subjects/%s/versions", c.URL, subj))
	}
	return sr, nil
}

func unmarshalRespErr(resp *http.Response, err error, into interface{}) error {
	if err != nil {
		return errors.Wrap(err, "making http request")
	}
	if resp.StatusCode != 200 {
		bod, err := io.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrap(err, "reading body")
		}
		errResp := &ErrorResponse{
			StatusCode: resp.StatusCode,
			Body:       string(bod),
		}
		return errResp
	}
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(into)
	if err != nil {
		return errors.Wrap(err, "unmarshaling body")
	}
	return nil
}
