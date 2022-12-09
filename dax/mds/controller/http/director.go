// Package http provides the http implementation of the Director interface.
package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/mds/controller"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// Ensure type implements interface.
var _ controller.Director = (*Director)(nil)

// Director is an http implementation of the Director interface.
type Director struct {
	// directivePath is the path portion of the URI to which directives should
	// be POSTed.
	directivePath string

	// snapshotRequestPath is the path portion of the URI to which snapshot
	// requests should be POSTed.
	snapshotRequestPath string

	client *http.Client

	logger logger.Logger
}

func NewDirector(cfg DirectorConfig) *Director {
	var logr = logger.NopLogger
	if cfg.Logger != nil {
		logr = cfg.Logger
	}

	return &Director{
		directivePath:       cfg.DirectivePath,
		snapshotRequestPath: cfg.SnapshotRequestPath,
		logger:              logr,
		client: &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   2 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   3 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		},
	}
}

type DirectorConfig struct {
	DirectivePath       string
	SnapshotRequestPath string
	Logger              logger.Logger
}

func (d *Director) SendDirective(ctx context.Context, dir *dax.Directive) error {
	url := fmt.Sprintf("%s/%s", dir.Address.WithScheme("http"), d.directivePath)
	d.logger.Printf("SEND HTTP directive to: %s\n", url)

	// Encode the request.
	postBody, err := json.Marshal(dir)
	if err != nil {
		return errors.Wrap(err, "marshalling directive to json")
	}
	requestBody := bytes.NewBuffer(postBody)

	// Post the request.
	request, _ := http.NewRequest(http.MethodPost, url, requestBody)
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")

	resp, err := d.client.Do(request)
	if err != nil {
		return errors.Wrap(err, "doing send directive")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	return nil
}

func (d *Director) SendSnapshotShardDataRequest(ctx context.Context, req *dax.SnapshotShardDataRequest) error {
	url := fmt.Sprintf("%s/%s/shard-data", req.Address.WithScheme("http"), d.snapshotRequestPath)
	d.logger.Printf("SEND HTTP snapshot shard data request to: %s\n", url)

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalling snapshot shard data request to json")
	}
	requestBody := bytes.NewBuffer(postBody)

	// Post the request.
	request, _ := http.NewRequest(http.MethodPost, url, requestBody)
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")

	resp, err := d.client.Do(request)
	if err != nil {
		return errors.Wrap(err, "doing snapshot shard data request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	return nil
}

func (d *Director) SendSnapshotTableKeysRequest(ctx context.Context, req *dax.SnapshotTableKeysRequest) error {
	url := fmt.Sprintf("%s/%s/table-keys", req.Address.WithScheme("http"), d.snapshotRequestPath)
	d.logger.Printf("SEND HTTP snapshot table keys request to: %s\n", url)

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalling snapshot table keys request to json")
	}
	requestBody := bytes.NewBuffer(postBody)

	// Post the request.
	request, _ := http.NewRequest(http.MethodPost, url, requestBody)
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")

	resp, err := d.client.Do(request)
	if err != nil {
		return errors.Wrap(err, "doing snapshot table keys request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	return nil
}

func (d *Director) SendSnapshotFieldKeysRequest(ctx context.Context, req *dax.SnapshotFieldKeysRequest) error {
	url := fmt.Sprintf("%s/%s/field-keys", req.Address.WithScheme("http"), d.snapshotRequestPath)
	d.logger.Printf("SEND HTTP snapshot field keys request to: %s\n", url)

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalling snapshot field keys request to json")
	}
	requestBody := bytes.NewBuffer(postBody)

	// Post the request.
	request, _ := http.NewRequest(http.MethodPost, url, requestBody)
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")

	resp, err := d.client.Do(request)
	if err != nil {
		return errors.Wrap(err, "doing snapshot field keys request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	return nil
}
