// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/molecula/featurebase/v3/authn"
	"golang.org/x/oauth2"

	"github.com/molecula/featurebase/v3/authz"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/pql"
)

// Test custom UnmarshalJSON for postIndexRequest object
func TestPostIndexRequestUnmarshalJSON(t *testing.T) {
	tests := []struct {
		json     string
		expected postIndexRequest
		err      string
	}{
		{json: `{"options": {}}`, expected: postIndexRequest{Options: IndexOptions{TrackExistence: true}}},
		{json: `{"options": {"trackExistence": false}}`, expected: postIndexRequest{Options: IndexOptions{TrackExistence: false}}},
		{json: `{"options": {"keys": true}}`, expected: postIndexRequest{Options: IndexOptions{Keys: true, TrackExistence: true}}},
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
	timeQuantum := TimeQuantum("YMD")
	defaultCacheSize := uint32(DefaultCacheSize)
	tests := []struct {
		json     string
		expected postFieldRequest
		err      string
	}{
		// FieldType: Set
		{json: `{"options": {}}`, expected: postFieldRequest{Options: fieldOptions{
			Type:      FieldTypeSet,
			CacheType: stringPtr(DefaultCacheType),
			CacheSize: &defaultCacheSize,
		}}},
		{json: `{"options": {"type": "set"}}`, expected: postFieldRequest{Options: fieldOptions{
			Type:      FieldTypeSet,
			CacheType: stringPtr(DefaultCacheType),
			CacheSize: &defaultCacheSize,
		}}},
		{json: `{"options": {"type": "set", "cacheType": "lru"}}`, expected: postFieldRequest{Options: fieldOptions{
			Type:      FieldTypeSet,
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
			Type: FieldTypeInt,
			Min:  decimalPtr(pql.NewDecimal(0, 0)),
			Max:  decimalPtr(pql.NewDecimal(1001, 0)),
		}}},
		{json: `{"options": {"type": "int", "min": 0, "max": 1000, "cacheType": "ranked"}}`, err: "cacheType does not apply to field type int"},
		{json: `{"options": {"type": "int", "min": 0, "max": 1000, "cacheSize": 1000}}`, err: "cacheSize does not apply to field type int"},
		{json: `{"options": {"type": "int", "min": 0, "max": 1000, "timeQuantum": "YMD"}}`, err: "timeQuantum does not apply to field type int"},

		// FieldType: Time
		{json: `{"options": {"type": "time"}}`, err: "timeQuantum is required for field type time"},
		{json: `{"options": {"type": "time", "timeQuantum": "YMD"}}`, expected: postFieldRequest{Options: fieldOptions{
			Type:        FieldTypeTime,
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

func TestAuthentication(t *testing.T) {
	type evaluate func(w *httptest.ResponseRecorder, data []byte)
	type endpoint func(w http.ResponseWriter, r *http.Request)
	var (
		ClientId         = "e9088663-eb08-41d7-8f65-efb5f54bbb71"
		ClientSecret     = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
		AuthorizeURL     = "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/authorize"
		TokenURL         = "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/token"
		GroupEndpointURL = "https://graph.microsoft.com/v1.0/me/transitiveMemberOf/microsoft.graph.group?$count=true"
		LogoutURL        = "https://login.microsoftonline.com/common/oauth2/v2.0/logout"
		Scopes           = []string{"https://graph.microsoft.com/.default", "offline_access"}
		SecretKey        = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
	)

	secretKey, _ := hex.DecodeString("DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF")

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
		SecretKey,
	)
	if err != nil {
		t.Errorf("building auth object%s", err)
	}

	h := Handler{
		logger:      logger.NewStandardLogger(os.Stdout),
		queryLogger: logger.NewStandardLogger(os.Stdout),
		auth:        a,
	}

	hOff := Handler{}

	// make a valid token
	tkn := jwt.New(jwt.SigningMethodHS256)
	claims := tkn.Claims.(jwt.MapClaims)
	claims["oid"] = "42"
	claims["name"] = "todd"
	validToken, err := tkn.SignedString([]byte(secretKey))
	if err != nil {
		t.Fatal(err)
	}
	validToken = "Bearer " + validToken

	token := oauth2.Token{
		TokenType:    "Bearer",
		AccessToken:  "asdf",
		RefreshToken: "abcdef",
		Expiry:       time.Now().Add(time.Hour),
	}

	// make an expired token
	claims["exp"] = "1"
	expiredToken, err := tkn.SignedString([]byte(secretKey))
	if err != nil {
		t.Fatal(err)
	}
	expiredToken = "Bearer " + expiredToken

	validCookie := &http.Cookie{
		Name:     "molecula-chip",
		Value:    token.AccessToken,
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		Expires:  token.Expiry,
	}

	permissions1 := `"user-groups":
  "dca35310-ecda-4f23-86cd-876aee559900":
    "test": "write"
admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe"`

	tests := []struct {
		name     string
		path     string
		kind     string
		method   string
		yamlData string
		token    string
		cookie   *http.Cookie
		handler  endpoint
		fn       evaluate
	}{
		{
			name:    "Login",
			path:    "/login",
			kind:    "type1",
			cookie:  validCookie,
			handler: h.handleLogin,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if strings.Index(string(data), AuthorizeURL) != 9 {
					t.Errorf("incorrect redirect url: expected: %s, got: %s", AuthorizeURL, string(data))
				}
			},
		},
		{
			name:    "Logout",
			path:    "/logout",
			kind:    "type1",
			cookie:  validCookie,
			handler: h.handleLogout,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if w.Result().Cookies()[0].Value != "" {
					t.Errorf("expected cookie to be cleared, got: %+v", w.Result().Cookies()[0].Value)
				}
			},
		},
		{
			name:    "Authenticate-ValidToken",
			path:    "/auth",
			kind:    "bearer",
			token:   validToken,
			handler: h.handleCheckAuthentication,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if w.Result().StatusCode != 200 {
					body, _ := readResponse(w)
					t.Errorf("expected http code 200, got: %+v with body: %+v", w.Result().StatusCode, body)
				}
			},
		},
		{
			name:    "Authenticate-NoToken",
			path:    "/auth",
			kind:    "type1",
			handler: h.handleCheckAuthentication,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				// not token at all == status forbidden
				if w.Result().StatusCode != 401 {
					t.Errorf("expected http code 401, got: %+v", w.Result().StatusCode)
				}
			},
		},
		{
			name:    "Authenticate-InvalidToken",
			path:    "/auth",
			kind:    "type1",
			token:   "this isn't a real token",
			handler: h.handleCheckAuthentication,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				// no valid token in header == Unauthorized
				if w.Result().StatusCode != http.StatusUnauthorized {
					t.Errorf("expected http code 401, got: %+v", w.Result().StatusCode)
				}
			},
		},
		{
			name:    "Authenticate-ExpiredToken",
			path:    "/auth",
			kind:    "type1",
			token:   expiredToken,
			handler: h.handleCheckAuthentication,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				// expired token == unauthorized
				if w.Result().StatusCode != 401 {
					t.Errorf("expected http code 403, got: %+v", w.Result().StatusCode)
				}
			},
		},
		{
			name:    "UserInfo",
			path:    "/userinfo",
			kind:    "bearer",
			token:   validToken,
			handler: h.handleUserInfo,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				uinfo := authn.UserInfo{}
				err = json.Unmarshal(data, &uinfo)
				if err != nil {
					t.Errorf("unmarshalling userinfo")
				}
				if uinfo.UserID != "42" && uinfo.UserName != "todd" {
					t.Errorf("expected http code 400, got: %+v", uinfo)
				}
			},
		},
		{
			name:    "UserInfo-NoCookie",
			path:    "/userinfo",
			kind:    "bearer",
			token:   "",
			handler: h.handleUserInfo,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if got := w.Result().StatusCode; got != http.StatusForbidden {
					t.Errorf("expected 403, got %v", got)
				}
			},
		},
		{
			name:    "Redirect-NoAuthCode",
			path:    "/redirect",
			kind:    "type1",
			cookie:  validCookie,
			handler: h.handleRedirect,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if strings.Index(string(data), AuthorizeURL) != 9 {
					if w.Result().StatusCode != 400 {
						t.Errorf("expected http code 400, got: %+v", w.Result().StatusCode)
					}
				}
			},
		},
		{
			name:    "Redirect-SomeAuthCode",
			path:    "/redirect",
			kind:    "type2",
			cookie:  validCookie,
			handler: h.handleRedirect,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if strings.Index(string(data), AuthorizeURL) != 9 {
					if w.Result().StatusCode != 400 {
						t.Errorf("expected http code 400, got: %+v", w.Result().StatusCode)
					}
				}
			},
		},
		{
			name:    "Login-AuthOff",
			path:    "/login",
			kind:    "type1",
			cookie:  validCookie,
			handler: hOff.handleLogin,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if strings.Index(string(data), AuthorizeURL) != 9 {
					if w.Result().StatusCode != 204 {
						t.Errorf("expected http code 204, got: %+v", w.Result().StatusCode)
					}
				}
			},
		},
		{
			name:    "Logout-AuthOff",
			path:    "/logout",
			kind:    "type1",
			cookie:  validCookie,
			handler: hOff.handleLogout,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if strings.Index(string(data), AuthorizeURL) != 9 {
					if w.Result().StatusCode != 204 {
						t.Errorf("expected http code 204, got: %+v", w.Result().StatusCode)
					}
				}
			},
		},
		{
			name:    "UserInfo-AuthOff",
			path:    "/userinfo",
			kind:    "type1",
			cookie:  validCookie,
			handler: hOff.handleUserInfo,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if strings.Index(string(data), AuthorizeURL) != 9 {
					if w.Result().StatusCode != 204 {
						t.Errorf("expected http code 204, got: %+v", w.Result().StatusCode)
					}
				}
			},
		},
		{
			name:    "Authenticate-AuthOff",
			path:    "/auth",
			kind:    "type1",
			cookie:  validCookie,
			handler: hOff.handleCheckAuthentication,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if strings.Index(string(data), AuthorizeURL) != 9 {
					if w.Result().StatusCode != 204 {
						t.Errorf("expected http code 204, got: %+v", w.Result().StatusCode)
					}
				}
			},
		},
		{
			name:    "Redirect-AuthOff",
			path:    "/redirect",
			kind:    "type1",
			cookie:  validCookie,
			handler: hOff.handleRedirect,
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if strings.Index(string(data), AuthorizeURL) != 9 {
					if w.Result().StatusCode != 204 {
						t.Errorf("expected http code 204, got: %+v", w.Result().StatusCode)
					}
				}
			},
		},
		{
			name:   "MW-AuthOff",
			path:   "/index/{index}/query",
			kind:   "middleware",
			cookie: validCookie,
			handler: func(w http.ResponseWriter, r *http.Request) {
				f := hOff.chkAuthZ(hOff.handlePostQuery, authz.Admin)
				f(w, r)
			},
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if w.Result().StatusCode != 400 {
					t.Errorf("expected http code 400, got: %+v", w.Result().StatusCode)
				}
			},
		},
		{
			name:   "MW-CreateIndexInsufficientPerms",
			path:   "/index/abcd",
			kind:   "bearer",
			method: http.MethodPost,
			token:  validToken,
			handler: func(w http.ResponseWriter, r *http.Request) {
				h := h
				var p authz.GroupPermissions
				if err := p.ReadPermissionsFile(strings.NewReader(permissions1)); err != nil {
					t.Errorf("Error: %s", err)
				}
				h.permissions = &p

				f := h.chkAuthZ(h.handlePostIndex, authz.Admin)
				f(w, r)
			},
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if got, want := w.Result().StatusCode, http.StatusForbidden; got != want {
					t.Errorf("expected %v, got %v", want, got)
				}
			},
		},
		{
			// this tests that there are no permissions read in even though
			// auth is turned on, so we get a 500
			name:  "MW-NoPermissions",
			path:  "/index/{index}/query",
			kind:  "bearer",
			token: validToken,
			handler: func(w http.ResponseWriter, r *http.Request) {
				h := h
				f := h.chkAuthZ(h.handlePostQuery, authz.Write)
				f(w, r)
			},
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if got, want := w.Result().StatusCode, http.StatusInternalServerError; got != want {
					t.Errorf("expected %v, got %v", want, got)
				}
			},
		},
		{
			name:  "MW-NoQuery",
			path:  "/index/{index}/query",
			kind:  "bearer",
			token: validToken,
			handler: func(w http.ResponseWriter, r *http.Request) {
				h := h
				var p authz.GroupPermissions
				if err := p.ReadPermissionsFile(strings.NewReader(permissions1)); err != nil {
					t.Errorf("Error: %s", err)
				}
				h.permissions = &p
				f := h.chkAuthZ(h.handlePostQuery, authz.Write)
				f(w, r)
			},
			fn: func(w *httptest.ResponseRecorder, data []byte) {
				if got, want := w.Result().StatusCode, http.StatusBadRequest; got != want {
					t.Errorf("expected %v, got: %+v", want, got)
				}
			},
		},
	}

	for _, test := range tests {
		switch test.kind {
		case "type1", "middleware":
			t.Run(test.name, func(t *testing.T) {
				r := httptest.NewRequest(http.MethodGet, test.path, nil)
				w := httptest.NewRecorder()
				if test.cookie != nil {
					r.AddCookie(test.cookie)
				}
				test.handler(w, r)
				data, err := readResponse(w)
				if err != nil {
					t.Errorf("expected no errors reading response, got: %+v", err)
				}
				test.fn(w, data)
			})
		case "type2":
			t.Run(test.name, func(t *testing.T) {
				r := httptest.NewRequest(http.MethodGet, test.path, nil)
				w := httptest.NewRecorder()
				r.Form = url.Values{}
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				r.Form.Add("code", "junk")

				test.handler(w, r)
				data, err := readResponse(w)
				if err != nil {
					t.Errorf("expected no errors reading response, got: %+v", err)
				}

				test.fn(w, data)

			})
		case "bearer":
			t.Run(test.name, func(t *testing.T) {
				if test.method == "" {
					test.method = http.MethodGet
				}
				r := httptest.NewRequest(test.method, test.path, nil)
				w := httptest.NewRecorder()
				if test.token != "" {
					r.Header.Add("Authorization", test.token)
				}
				test.handler(w, r)
				data, err := readResponse(w)
				if err != nil {
					t.Errorf("expected no errors reading response, got: %+v", err)
				}
				test.fn(w, data)
			})
		}

	}

}

func TestChkAuthN(t *testing.T) {
	a := NewTestAuth(t)
	h := Handler{
		logger:      logger.NewStandardLogger(os.Stdout),
		queryLogger: logger.NewStandardLogger(os.Stdout),
		auth:        a,
	}

	// make a valid token
	tkn := jwt.New(jwt.SigningMethodHS256)
	claims := tkn.Claims.(jwt.MapClaims)
	claims["oid"] = "42"
	claims["name"] = "A. Token"
	validToken, err := tkn.SignedString(a.SecretKey())
	if err != nil {
		t.Fatal(err)
	}
	validToken = "Bearer " + validToken

	// make an invalid token
	invalidToken := "Bearer " + "thisis.a.bad.token"

	// make an expired token
	claims["exp"] = "1"
	expiredToken, err := tkn.SignedString(a.SecretKey())
	if err != nil {
		t.Fatal(err)
	}
	expiredToken = "Bearer " + expiredToken

	testingHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("good"))
	}

	cases := []struct {
		name       string
		endpoint   string
		token      string
		handler    http.HandlerFunc
		statusCode int
	}{
		{
			name:       "Valid",
			token:      validToken,
			handler:    h.chkAuthN(testingHandler),
			statusCode: http.StatusOK,
		},
		{
			name:       "Invalid",
			token:      invalidToken,
			handler:    h.chkAuthN(testingHandler),
			statusCode: http.StatusUnauthorized,
		},
		{
			name:       "Expired",
			token:      expiredToken,
			handler:    h.chkAuthN(testingHandler),
			statusCode: http.StatusUnauthorized,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/whatever", nil)
			r.Header.Add("Authorization", test.token)
			test.handler(w, r)
			resp := w.Result()
			if resp.StatusCode != test.statusCode {
				t.Fatalf("expected %v, got %v", test.statusCode, resp.StatusCode)
			}
		})
	}
}

func TestChkInternal(t *testing.T) {
	a := NewTestAuth(t)
	authKey := "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
	h := Handler{
		logger:      logger.NewStandardLogger(os.Stdout),
		queryLogger: logger.NewStandardLogger(os.Stdout),
		auth:        a,
	}

	testingHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("good"))
	}

	cases := []struct {
		name       string
		statusCode int
		handler    http.HandlerFunc
		key        string
	}{
		{
			name:       "happyPath",
			statusCode: http.StatusOK,
			handler:    h.chkInternal(testingHandler),
			key:        authKey,
		},
		{
			name:       "unhappyPath-empty",
			statusCode: http.StatusUnauthorized,
			handler:    h.chkInternal(testingHandler),
			key:        "",
		},
		{
			name:       "unhappyPath-wrong",
			statusCode: http.StatusUnauthorized,
			handler:    h.chkInternal(testingHandler),
			key:        "BEABBEEFBEABBEEFBEABBEEFBEABBEEFBEABBEEFBEABBEEFBEABBEEFBEABBEEF",
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/whatever", nil)
			if test.key != "" {
				r.Header.Add("X-Feature-Key", test.key)
			}
			test.handler(w, r)
			resp := w.Result()
			if resp.StatusCode != test.statusCode {
				t.Fatalf("expected %v, got %v", test.statusCode, resp.StatusCode)
			}
		})
	}
}

func NewTestAuth(t *testing.T) *authn.Auth {
	t.Helper()
	var (
		ClientID         = "e9088663-eb08-41d7-8f65-efb5f54bbb71"
		ClientSecret     = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
		AuthorizeURL     = "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/authorize"
		TokenURL         = "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/token"
		GroupEndpointURL = "https://graph.microsoft.com/v1.0/me/transitiveMemberOf/microsoft.graph.group?$count=true"
		LogoutURL        = "https://login.microsoftonline.com/common/oauth2/v2.0/logout"
		Scopes           = []string{"https://graph.microsoft.com/.default", "offline_access"}
		Key              = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
	)

	a, err := authn.NewAuth(
		logger.NewStandardLogger(os.Stdout),
		"http://localhost:10101/",
		Scopes,
		AuthorizeURL,
		TokenURL,
		GroupEndpointURL,
		LogoutURL,
		ClientID,
		ClientSecret,
		Key,
	)
	if err != nil {
		t.Fatalf("building auth object%s", err)
	}
	return a
}
