package ctl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"golang.org/x/oauth2"
)

func TestFormatPromptBox(t *testing.T) {
	golden := `+----------------------------------------------------+
|                                                    |
| Please visit:                                      |
| testingtestingtestingtestingtestingtestingtesting  |
|                                                    |
| And enter the code:                                |
| blahblah                                           |
|                                                    |
+----------------------------------------------------+
`
	got := formatPromptBox("testingtestingtestingtestingtestingtestingtesting", "blahblah")
	if got != golden {
		t.Fatalf("expected:\n%s, got:\n%s", golden, got)
	}
}

type authReqTest struct {
	config oauth2.Config
	expRsp *deviceAuthResponse
	expErr error
}

func TestDeviceAuthRequest(t *testing.T) {
	goodClientID := "ring-a-ding-dillo"
	goodResponse := deviceAuthResponse{
		DeviceCode:              "Old knives are long enough as swords for hobbit-people.",
		UserCode:                "Sharp blades are good to have, if Shire-folk go walking, east, south, or far away into dark and danger.",
		VerificationURI:         "I am no weather-master, nor is aught that goes on two legs.",
		VerificationURIComplete: "Hey! Come merry dol! derry dol! My hearties!",
		ExpiresIn:               -486846000,
		Interval:                9,
	}
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			clientID := r.PostFormValue("client_id")
			if clientID != goodClientID {
				http.Error(
					w,
					"Get out, you old Wight! Vanish in the sunlight!",
					http.StatusBadRequest,
				)
				return
			}
			b := bytes.Buffer{}
			if err := json.NewEncoder(&b).Encode(goodResponse); err != nil {
				t.Fatalf("unexpected error encoding goodResponse: %v", err)
			}
			w.Write(b.Bytes())
			w.WriteHeader(http.StatusOK)
		}),
	)
	cli := &http.Client{}
	for name, test := range map[string]authReqTest{
		"badRequest": {
			config: oauth2.Config{
				ClientID: "Iarwain Ben-adar",
				Endpoint: oauth2.Endpoint{AuthURL: srv.URL},
			},
			expRsp: nil,
			expErr: fmt.Errorf("unsuccessful with status 400: Get out, you old Wight! Vanish in the sunlight!"),
		},
		"goodRequest": {
			config: oauth2.Config{
				ClientID: "ring-a-ding-dillo",
				Endpoint: oauth2.Endpoint{AuthURL: srv.URL},
			},
			expRsp: &goodResponse,
			expErr: nil,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := deviceAuthRequest(cli, test.config)
			if !errEqual(err, test.expErr) {
				t.Errorf("expected '%v', got '%v'", test.expErr, err)
			}
			if !reflect.DeepEqual(got, test.expRsp) {
				t.Errorf("expected '%v', got '%v'", test.expRsp, got)
			}
		})
	}
}

type respTest struct {
	resp *http.Response
	exp  interface{}
}

func TestParseResponse(t *testing.T) {
	sr := successResponse{
		Access:    "ACCESS",
		Refresh:   "REFRESH",
		Type:      "access",
		ExpiresIn: 10000,
		Scope:     "scopity-scope-scopity-scope-pope",
	}
	good := bytes.Buffer{}
	if err := json.NewEncoder(&good).Encode(sr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sd := errorResponse{Error: "slow_down"}
	slowDown := bytes.Buffer{}
	if err := json.NewEncoder(&slowDown).Encode(sd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sd.Error = "authorization_pending"
	pending := bytes.Buffer{}
	if err := json.NewEncoder(&pending).Encode(sd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sd.Error = "invalid_client"
	genErr := bytes.Buffer{}
	if err := json.NewEncoder(&genErr).Encode(sd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for name, test := range map[string]respTest{
		"success": {
			resp: &http.Response{
				StatusCode:    200,
				Body:          io.NopCloser(&good),
				ContentLength: int64(good.Len()),
			},
			exp: sr,
		},
		"slowDown": {
			resp: &http.Response{
				StatusCode:    400,
				Body:          io.NopCloser(&slowDown),
				ContentLength: int64(slowDown.Len()),
			},
			exp: waitResponse{SlowDown: true},
		},
		"pending": {
			resp: &http.Response{
				StatusCode:    400,
				Body:          io.NopCloser(&pending),
				ContentLength: int64(pending.Len()),
			},
			exp: waitResponse{},
		},
		"generalError": {
			resp: &http.Response{
				StatusCode:    400,
				Body:          io.NopCloser(&genErr),
				ContentLength: int64(pending.Len()),
			},
			exp: errorResponse{
				Error:       "invalid_client",
				Description: "",
				URI:         "",
				Err: fmt.Errorf("error: %s, description: %s, uri: %s", "invalid_client",
					"", ""),
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := parseResponse(test.resp); !reflect.DeepEqual(got, test.exp) {
				t.Errorf("expected: '%v', got '%v'", test.exp, got)
			}
		})
	}
}

func errEqual(a, b error) bool {
	if a == nil {
		return b == nil
	}
	if b == nil {
		return a == nil
	}
	return a.Error() == b.Error()
}
