// Copyright 2021 Molecula Corp. All rights reserved.
package http

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	gohttp "net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/securecookie"
	pilosa "github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/authn"
	"github.com/molecula/featurebase/v2/logger"
	"github.com/molecula/featurebase/v2/pql"
	"golang.org/x/oauth2"
)

// Test custom UnmarshalJSON for postIndexRequest object
func TestPostIndexRequestUnmarshalJSON(t *testing.T) {
	tests := []struct {
		json     string
		expected postIndexRequest
		err      string
	}{
		{json: `{"options": {}}`, expected: postIndexRequest{Options: pilosa.IndexOptions{TrackExistence: true}}},
		{json: `{"options": {"trackExistence": false}}`, expected: postIndexRequest{Options: pilosa.IndexOptions{TrackExistence: false}}},
		{json: `{"options": {"keys": true}}`, expected: postIndexRequest{Options: pilosa.IndexOptions{Keys: true, TrackExistence: true}}},
		{json: `{"options": 4}`, err: "options is not map[string]interface{}"},
		{json: `{"option": {}}`, err: "unknown key: option:map[]"},
		{json: `{"options": {"badKey": "test"}}`, err: "unknown key: badKey:test"},
	}
	for _, test := range tests {
		actual := &postIndexRequest{}
		err := json.Unmarshal([]byte(test.json), actual)

		if err != nil {
			if test.err == "" || test.err != err.Error() {
				t.Errorf("expected error: %v, but got result: %v", test.err, err)
			}
		} else {
			if test.err != "" {
				t.Errorf("expected error: %v, but got no error", test.err)
			}
		}

		if test.err == "" {
			if !reflect.DeepEqual(*actual, test.expected) {
				t.Errorf("expected: %v, but got: %v for JSON: %s", test.expected, *actual, test.json)
			}
		}
	}
}

// Test custom UnmarshalJSON for postFieldRequest object
func TestPostFieldRequestUnmarshalJSON(t *testing.T) {
	foo := "foo"
	tests := []struct {
		json     string
		expected postFieldRequest
		err      string
	}{
		{json: `{"options": {}}`, expected: postFieldRequest{}},
		{json: `{"options": 4}`, err: "json: cannot unmarshal number"},
		{json: `{"option": {}}`, err: `json: unknown field "option"`},
		{json: `{"options": {"badKey": "test"}}`, err: `json: unknown field "badKey"`},
		{json: `{"options": {"inverseEnabled": true}}`, err: `json: unknown field "inverseEnabled"`},
		{json: `{"options": {"cacheType": "foo"}}`, expected: postFieldRequest{Options: fieldOptions{CacheType: &foo}}},
		{json: `{"options": {"inverse": true, "cacheType": "foo"}}`, err: `json: unknown field "inverse"`},
	}
	for i, test := range tests {
		actual := &postFieldRequest{}
		dec := json.NewDecoder(bytes.NewReader([]byte(test.json)))
		dec.DisallowUnknownFields()
		err := dec.Decode(actual)
		if err != nil {
			if test.err == "" || !strings.HasPrefix(err.Error(), test.err) {
				t.Errorf("test %d: expected error: %v, but got result: %v", i, test.err, err)
			}
		}

		if test.err == "" {
			if !reflect.DeepEqual(*actual, test.expected) {
				t.Errorf("test %d: expected: %v, but got: %v", i, test.expected, *actual)
			}
		}
	}
}

func stringPtr(s string) *string {
	return &s
}

func decimalPtr(d pql.Decimal) *pql.Decimal {
	return &d
}

// Test fieldOption validation.
func TestFieldOptionValidation(t *testing.T) {
	timeQuantum := pilosa.TimeQuantum("YMD")
	defaultCacheSize := uint32(pilosa.DefaultCacheSize)
	tests := []struct {
		json     string
		expected postFieldRequest
		err      string
	}{
		// FieldType: Set
		{json: `{"options": {}}`, expected: postFieldRequest{Options: fieldOptions{
			Type:      pilosa.FieldTypeSet,
			CacheType: stringPtr(pilosa.DefaultCacheType),
			CacheSize: &defaultCacheSize,
		}}},
		{json: `{"options": {"type": "set"}}`, expected: postFieldRequest{Options: fieldOptions{
			Type:      pilosa.FieldTypeSet,
			CacheType: stringPtr(pilosa.DefaultCacheType),
			CacheSize: &defaultCacheSize,
		}}},
		{json: `{"options": {"type": "set", "cacheType": "lru"}}`, expected: postFieldRequest{Options: fieldOptions{
			Type:      pilosa.FieldTypeSet,
			CacheType: stringPtr("lru"),
			CacheSize: &defaultCacheSize,
		}}},
		{json: `{"options": {"type": "set", "min": 0}}`, err: "min does not apply to field type set"},
		{json: `{"options": {"type": "set", "max": 100}}`, err: "max does not apply to field type set"},
		{json: `{"options": {"type": "set", "timeQuantum": "YMD"}}`, err: "timeQuantum does not apply to field type set"},

		// FieldType: Int
		{json: `{"options": {"type": "int"}}`, err: "min is required for field type int"},
		{json: `{"options": {"type": "int", "min": 0}}`, err: "max is required for field type int"},
		{json: `{"options": {"type": "int", "min": 0, "max": 1001}}`, expected: postFieldRequest{Options: fieldOptions{
			Type: pilosa.FieldTypeInt,
			Min:  decimalPtr(pql.NewDecimal(0, 0)),
			Max:  decimalPtr(pql.NewDecimal(1001, 0)),
		}}},
		{json: `{"options": {"type": "int", "min": 0, "max": 1000, "cacheType": "ranked"}}`, err: "cacheType does not apply to field type int"},
		{json: `{"options": {"type": "int", "min": 0, "max": 1000, "cacheSize": 1000}}`, err: "cacheSize does not apply to field type int"},
		{json: `{"options": {"type": "int", "min": 0, "max": 1000, "timeQuantum": "YMD"}}`, err: "timeQuantum does not apply to field type int"},

		// FieldType: Time
		{json: `{"options": {"type": "time"}}`, err: "timeQuantum is required for field type time"},
		{json: `{"options": {"type": "time", "timeQuantum": "YMD"}}`, expected: postFieldRequest{Options: fieldOptions{
			Type:        pilosa.FieldTypeTime,
			TimeQuantum: &timeQuantum,
		}}},
		{json: `{"options": {"type": "time", "timeQuantum": "YMD", "min": 0}}`, err: "min does not apply to field type time"},
		{json: `{"options": {"type": "time", "timeQuantum": "YMD", "max": 1000}}`, err: "max does not apply to field type time"},
		{json: `{"options": {"type": "time", "timeQuantum": "YMD", "cacheType": "ranked"}}`, err: "cacheType does not apply to field type time"},
		{json: `{"options": {"type": "time", "timeQuantum": "YMD", "cacheSize": 1000}}`, err: "cacheSize does not apply to field type time"},
	}
	for i, test := range tests {
		actual := &postFieldRequest{}
		dec := json.NewDecoder(bytes.NewReader([]byte(test.json)))
		dec.DisallowUnknownFields()
		err := dec.Decode(actual)
		if err != nil {
			t.Errorf("test %d: %v", i, err)
		}

		// Validate field options.
		if err := actual.Options.validate(); err != nil {
			if test.err == "" || test.err != err.Error() {
				t.Errorf("test %d: expected error: %v, but got result: %v", i, test.err, err)
			}
		}

		if test.err == "" {
			if !reflect.DeepEqual(*actual, test.expected) {
				t.Errorf("test %d: expected: %v, but got: %v", i, test.expected, *actual)
			}
		}
	}
}

func readResponse(w *httptest.ResponseRecorder) ([]byte, error) {
	res := w.Result()
	defer res.Body.Close()
	return ioutil.ReadAll(res.Body)
}

func TestAuth(t *testing.T) {
	var (
		ClientId         = "e9088663-eb08-41d7-8f65-efb5f54bbb71"
		ClientSecret     = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
		AuthorizeURL     = "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/authorize"
		TokenURL         = "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/token"
		GroupEndpointURL = "https://graph.microsoft.com/v1.0/me/transitiveMemberOf/microsoft.graph.group?$count=true"
		LogoutURL        = "https://login.microsoftonline.com/common/oauth2/v2.0/logout"
		Scopes           = []string{"https://graph.microsoft.com/.default", "offline_access"}
		HashKey          = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
		BlockKey         = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
	)

	hashKey, _ := hex.DecodeString("DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF")
	blockKey, _ := hex.DecodeString("DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF")

	a, err := authn.NewAuth(
		logger.NewStandardLogger(os.Stdout),
		"http://localhost:10101/",
		Scopes,
		AuthorizeURL,
		TokenURL,
		GroupEndpointURL,
		LogoutURL,
		ClientId,
		ClientSecret,
		HashKey,
		BlockKey,
	)
	if err != nil {
		t.Errorf("building auth object%s", err)
	}

	h := Handler{
		auth: a,
	}

	validToken := oauth2.Token{
		TokenType:    "Bearer",
		RefreshToken: "abcdef",
		Expiry:       time.Now().Add(time.Hour),
	}

	// emptyToken := oauth2.Token{}

	grp := authn.Group{
		UserID:    "snowstorm",
		GroupID:   "abcd123-A",
		GroupName: "Romantic Painters",
	}

	validCV := authn.CookieValue{
		UserID:          "snowstorm",
		UserName:        "J.M.W. Turner",
		GroupMembership: []authn.Group{grp},
		Token:           &validToken,
	}

	secure := securecookie.New(hashKey, blockKey)
	validEncodedCV, _ := secure.Encode("molecula-chip", validCV)
	validCookie := &gohttp.Cookie{
		Name:     "molecula-chip",
		Value:    validEncodedCV,
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		Expires:  validToken.Expiry,
	}

	t.Run("Login", func(t *testing.T) {
		r := httptest.NewRequest(gohttp.MethodGet, "/login", nil)
		w := httptest.NewRecorder()

		//login w/o cookie
		h.handleLogin(w, r)
		data, err := readResponse(w)
		if err != nil {
			t.Errorf("expected no errors reading response, got: %+v", err)
		}

		if strings.Index(string(data), AuthorizeURL) != 9 {
			t.Errorf("incorrect redirect url: expected: %s, got: %s", AuthorizeURL, string(data))
		}
	})

	t.Run("Logout", func(t *testing.T) {
		r := httptest.NewRequest(gohttp.MethodGet, "/logout", nil)
		w := httptest.NewRecorder()
		r.AddCookie(validCookie)

		h.handleLogout(w, r)

		if w.Result().Cookies()[0].Value != "" {
			t.Errorf("expected cookie to be cleared, got: %+v", w.Result().Cookies()[0].Value)
		}
	})

	t.Run("Redirect-NoAuthCode", func(t *testing.T) {
		r := httptest.NewRequest(gohttp.MethodGet, "/redirect", nil)
		w := httptest.NewRecorder()

		h.handleRedirect(w, r)

		if w.Result().StatusCode != 400 {
			t.Errorf("expected http code 400, got: %+v", w.Result().StatusCode)
		}

	})

	t.Run("Redirect-SomeAuthCode", func(t *testing.T) {
		r := httptest.NewRequest(gohttp.MethodGet, "/redirect", nil)
		w := httptest.NewRecorder()

		r.Form = url.Values{}
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Form.Add("code", "junk")

		h.handleRedirect(w, r)

		if w.Result().StatusCode != 400 {
			t.Errorf("expected http code 400, got: %+v", w.Result().StatusCode)
		}

	})
	t.Run("Authenticate-Cookie", func(t *testing.T) {
		r := httptest.NewRequest(gohttp.MethodGet, "/authenticate", nil)
		w := httptest.NewRecorder()
		r.AddCookie(validCookie)

		fmt.Printf("r %+v \n\n", r)

		h.handleCheckAuthentication(w, r)

		fmt.Printf("w %+v \n\n", w)

		//auth with cookie
		//auth w/o cookie

	})

	t.Run("GetUserInfo", func(t *testing.T) {
		r := httptest.NewRequest(gohttp.MethodGet, "/userinfo", nil)
		w := httptest.NewRecorder()

		h.handleUserInfo(w, r)

		data, err := readResponse(w)
		if err != nil {
			t.Errorf("expected no errors reading response, got: %+v", err)
		}

		uinfo := authn.UserInfo{}

		err = json.Unmarshal(data, &uinfo)
		if err != nil {
			t.Errorf("unmarshalling userinfo")
		}

		if uinfo.UserID != "" && uinfo.UserName != "" {

			t.Errorf("expected http code 400, got: %+v", uinfo)
		}

	})

	t.Run("GetUserInfo", func(t *testing.T) {
		r := httptest.NewRequest(gohttp.MethodGet, "/userinfo", nil)
		w := httptest.NewRecorder()
		r.AddCookie(validCookie)

		h.handleUserInfo(w, r)

		data, err := readResponse(w)
		if err != nil {
			t.Errorf("expected no errors reading response, got: %+v", err)
		}

		uinfo := authn.UserInfo{}

		err = json.Unmarshal(data, &uinfo)
		if err != nil {
			t.Errorf("unmarshalling userinfo")
		}

		if uinfo.UserID != "snowstorm" && uinfo.UserName != "J.M.W. Turner" {

			t.Errorf("expected http code 400, got: %+v", uinfo)
		}

	})

}
