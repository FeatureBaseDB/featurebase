// Copyright 2022 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/server"
	"golang.org/x/oauth2"
)

// AuthTokenCommand represents a command for retrieving an auth-token from a
// FeatureBase node.
type AuthTokenCommand struct { // nolint: maligned
	// TLS configuration.
	TLS       server.TLSConfig
	tlsConfig *tls.Config

	// Host is the host and port of the FeatureBase node to authenticate with.
	Host string `json:"host"`

	// Reusable client.
	client *pilosa.InternalClient

	// Standard input/output.
	*pilosa.CmdIO
}

// NewAuthTokenCommand returns a new instance of AuthTokenCommand.
func NewAuthTokenCommand(stdin io.Reader, stdout, stderr io.Writer) *AuthTokenCommand {
	return &AuthTokenCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

type deviceAuthResponse struct {
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
	ExpiresIn               int    `json:"expires_in"`
	Interval                int    `json:"interval"`
}

// deviceAuthRequest makes a Device Authorization Request and parses the Device
// Authorization Response as defined in RFC 8628 sections 3.1 and 3.2.
func deviceAuthRequest(cli *http.Client, config oauth2.Config) (rsp *deviceAuthResponse, err error) {
	// build the request
	req, err := http.NewRequest(
		http.MethodPost,
		pilosa.ReplaceFirstFromBack(config.Endpoint.AuthURL, "authorize", "devicecode"),
		strings.NewReader(
			url.Values{
				"client_id": {config.ClientID},
				"scope":     {strings.Join(config.Scopes, " ")},
			}.Encode(),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// do the request
	dar := &deviceAuthResponse{}
	if resp, err := cli.Do(req); err != nil {
		return nil, fmt.Errorf("making request with status %d: %v", resp.StatusCode, err)
	} else if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		body, _ := io.ReadAll(resp.Body) // ignore the error bc it's ok if there's no body
		return nil, fmt.Errorf(
			"unsuccessful with status %d: %s",
			resp.StatusCode,
			strings.TrimSpace(string(body)),
		)
	} else if err := json.NewDecoder(resp.Body).Decode(dar); err != nil {
		return nil, fmt.Errorf("decoding device auth response body: %w", err)
	}

	return dar, nil
}

// successResponse represents a successful device auth token reponse.
type successResponse struct {
	Access    string `json:"access_token"`
	Refresh   string `json:"refresh_token"`
	Type      string `json:"token_type"`
	ExpiresIn int    `json:"expires_in"`
	Scope     string `json:"scope"`
}

// errorResponse represents a device auth token error reponse.
type errorResponse struct {
	Err         error
	Error       string `json:"error"`
	Description string `json:"error_description"`
	URI         string `json:"error_uri"`
}

// waitResponse represents a device auth token reponse that indicates the client
// should continue polling.
type waitResponse struct {
	SlowDown bool
}

// parseResponse parses a device auth grant response from the /token endpoint. It
// returns a successResponse on success, an errorResponse on error, or a
// waitResponse if the authorization is still pending or we are requested to slow
// down.
func parseResponse(resp *http.Response) interface{} {
	if s := resp.StatusCode; s >= 200 && s < 300 {
		rsp := &successResponse{}
		if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
			return errorResponse{Err: fmt.Errorf("reading response body: %w", err)}
		}
		return *rsp
	}

	rsp := &errorResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return errorResponse{Err: fmt.Errorf("reading response body: %w", err)}
	}

	switch rsp.Error {
	case "slow_down":
		return waitResponse{SlowDown: true}
	case "authorization_pending":
		return waitResponse{}
	default:
		rsp.Err = fmt.Errorf("error: %s, description: %s, uri: %s", rsp.Error, rsp.Description, rsp.URI)
		return *rsp
	}
}

// Run executes the main program execution.
func (cmd *AuthTokenCommand) Run(ctx context.Context) (err error) {
	// Parse TLS configuration for node-specific clients.
	tls := cmd.TLSConfiguration()
	if cmd.tlsConfig, err = server.GetTLSConfig(&tls, cmd.Logger()); err != nil {
		return fmt.Errorf("parsing tls config: %w", err)
	}

	// Create an internal client.
	client, err := commandClient(cmd)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	// Get the OAuth config.
	var access, refresh string
	config, err := client.OAuthConfig()
	if err != nil {
		return fmt.Errorf("getting oauth config: %w", err)
	}
	cli := &http.Client{}

	// Make the device authorization request.
	dar, err := deviceAuthRequest(cli, config)
	if err != nil {
		return fmt.Errorf("making device auth request: %w", err)
	}

	// Prompt the user to visit verification_uri and enter code.
	fmt.Printf(formatPromptBox(dar.VerificationURI, dar.UserCode))

	// Request a token until success or error response, slowing down if requested.
	interval := dar.Interval
	var exit bool

	for !exit {
		time.Sleep(time.Duration(interval) * time.Second)

		// Make a device access token request to the token endpoint.
		req, err := http.NewRequest(
			http.MethodPost,
			config.Endpoint.TokenURL,
			strings.NewReader(
				url.Values{
					"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
					"client_id":   {config.ClientID},
					"device_code": {dar.DeviceCode},
				}.Encode(),
			),
		)
		if err != nil {
			return fmt.Errorf("building request to %s: %w", config.Endpoint.TokenURL, err)
		}

		resp, err := cli.Do(req)
		if err != nil {
			return fmt.Errorf("making request to %s: %w", config.Endpoint.TokenURL, err)
		}

		output := parseResponse(resp)

		switch o := output.(type) {
		case successResponse:
			access = o.Access
			refresh = o.Refresh
			exit = true
		case errorResponse:
			return o.Err
		case waitResponse:
			if o.SlowDown {
				interval += 5
			}
		}
	}

	// Convey the response to the user.
	fmt.Printf("\nauth-token: %s\n", access)
	fmt.Printf("\nrefresh-token: %s\n", refresh)
	return nil
}

// TLSHost implements the CommandWithTLSSupport interface.
func (cmd *AuthTokenCommand) TLSHost() string { return cmd.Host }

// TLSConfiguration implements the CommandWithTLSSupport interface.
func (cmd *AuthTokenCommand) TLSConfiguration() server.TLSConfig { return cmd.TLS }

// formatPromptBox makes a nice little prompt box to hold the url and code.
func formatPromptBox(url, code string) string {
	top := "+-----------------------------------------------"
	buffer := "|                                               "
	width := len(top) + 1
	if len(url) >= width {
		width = len(url) + 5
	}
	if len(code) >= width {
		width = len(code) + 5
	}

	urlStr := fmt.Sprintf("| %s", url)
	for i := len(urlStr); i < width-1; i++ {
		urlStr += " "
	}
	urlStr += "|"

	codeStr := fmt.Sprintf("| %s", code)
	for i := len(codeStr); i < width-1; i++ {
		codeStr += " "
	}
	codeStr += "|"

	for i := len(top); i < width-1; i++ {
		top += "-"
		buffer += " "
	}
	top += "+\n"
	buffer += "|\n"

	pls := "| Please visit:"
	for i := len(pls); i < width-1; i++ {
		pls += " "
	}
	pls += "|\n"

	and := "| And enter the code:"
	for i := len(and); i < width-1; i++ {
		and += " "
	}
	and += "|\n"

	return fmt.Sprintf("%s%s%s%s\n%s%s%s\n%s%s", top, buffer, pls, urlStr, buffer, and, codeStr, buffer, top)
}
