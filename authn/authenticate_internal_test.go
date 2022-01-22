package authn

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/pkg/errors"
)

func NewTestAuth(t *testing.T) *Auth {
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

	a, err := NewAuth(
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

func TestAuth(t *testing.T) {
	a := NewTestAuth(t)
	t.Run("SetCookie", func(t *testing.T) {
		w := httptest.NewRecorder()
		err := a.setCookie(w, "a cookie value", time.Now().Add(time.Hour))
		if err != nil {
			t.Fatalf("expected no errors, got: %v", err)
		}

		if w.Result().Cookies()[0].Value == "" {
			t.Errorf("expected something, got empty string")
		}

		if got, want := w.Result().Cookies()[0].Path, "/"; got != want {
			t.Fatalf("path=%s, want %s", got, want)
		}
	})
	t.Run("KeyLength", func(t *testing.T) {
		_, err := NewAuth(
			logger.NewStandardLogger(os.Stdout),
			"http://localhost:10101/",
			[]string{"https://graph.microsoft.com/.default", "offline_access"},
			"https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/authorize",
			"https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/token",
			"https://graph.microsoft.com/v1.0/me/transitiveMemberOf/microsoft.graph.group?$count=true",
			"https://login.microsoftonline.com/common/oauth2/v2.0/logout",
			"e9088663-eb08-41d7-8f65-efb5f54bbb71",
			"DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF",
			"DEADBEEFD",
		)
		if err == nil || !strings.Contains(err.Error(), "decoding secret key") {
			t.Fatalf("expected error decoding secret key got: %v", err)
		}
	})
	t.Run("GetSecretKey", func(t *testing.T) {
		want, _ := hex.DecodeString("DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF")
		if got := a.SecretKey(); !bytes.Equal(got, want) {
			t.Fatalf("expected %v, got %v", got, want)
		}
	})
	cases := []struct {
		name   string
		uid    string
		uname  string
		exp    interface{}
		groups []Group
		err    error
	}{
		{
			name:  "GoodToken",
			uid:   "42",
			uname: "A. Token",
			groups: []Group{
				{
					GroupID:   "ac97c9e2-346b-42a2-b6da-18bcb61a32fe",
					GroupName: "adminGroup",
				},
			},
		},
		{
			name:  "ExpiredToken",
			uid:   "42",
			uname: "A. Token",
			groups: []Group{
				{
					GroupID:   "ac97c9e2-346b-42a2-b6da-18bcb61a32fe",
					GroupName: "adminGroup",
				},
			},
			exp: "-17764800",
			err: errors.Wrap(fmt.Errorf("Token is expired"), "parsing bearer token"),
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			tkn := jwt.New(jwt.SigningMethodHS256)
			claims := tkn.Claims.(jwt.MapClaims)
			groupString, err := ToGob64(test.groups)
			if err != nil {
				t.Fatalf("unexpected error when gobbing groups %v", err)
			}
			claims["molecula-idp-groups"] = groupString
			claims["oid"] = test.uid
			claims["name"] = test.uname
			if test.exp != nil {
				claims["exp"] = test.exp
			}
			token, err := tkn.SignedString(a.SecretKey())
			if err != nil {
				t.Fatalf("unexpected error when signing token %v", err)
			}

			uinfo, err := a.Authenticate(token)
			// okay this part kind of sucks bc we need to check errors and i
			// dont want to write a whole new test for things that should have
			// errors just to avoid this mess. errors.Is doesn't work either
			if (test.err == nil && err != nil) || (test.err != nil && err == nil) {
				t.Fatalf("expected %v, but got %v", test.err, err)
			} else if test.err != nil && err != nil {
				if test.err.Error() != err.Error() {
					t.Fatalf("expected %v, but got %v", test.err, err)
				} else {
					return
				}
			}

			if !reflect.DeepEqual(uinfo.Groups, test.groups) {
				t.Fatalf("expected %v, got %v", test.groups, uinfo.Groups)
			}
			if !reflect.DeepEqual(uinfo.UserID, test.uid) {
				t.Fatalf("expected %v, got %v", test.uid, uinfo.UserID)
			}
			if !reflect.DeepEqual(uinfo.UserName, test.uname) {
				t.Fatalf("expected %v, got %v", test.uname, uinfo.UserName)
			}
		})
	}
}

func TestGobs(t *testing.T) {
	t.Run("goodGob!", func(t *testing.T) {
		g := []Group{
			{
				GroupID:   "groupA",
				GroupName: "groupA-Name",
			},
			{
				GroupID:   "groupB",
				GroupName: "groupB-Name",
			},
			{
				GroupID:   "groupC",
				GroupName: "groupC-Name",
			},
		}
		gobbed, err := ToGob64(g)
		if err != nil {
			t.Fatalf("could not gob %+v", g)
		}
		ungobbed, err := FromGob64(gobbed)
		if err != nil {
			t.Fatalf("could not ungob %+v", gobbed)
		}
		if !reflect.DeepEqual(ungobbed, g) {
			t.Fatalf("expected %v, got %v", g, ungobbed)
		}
	})
}

func TestDecodeHex(t *testing.T) {
	t.Run("cantDecode", func(t *testing.T) {
		_, err := decodeHex("gggg")
		if err == nil {
			t.Fatalf("expected err cannot decode slice, got nil")
		}
	})
	t.Run("tooSmall", func(t *testing.T) {
		_, err := decodeHex("DEADBEEF")
		if err == nil {
			t.Fatalf("expected err wrong length, got nil")
		}
	})
	t.Run("tooBig", func(t *testing.T) {
		_, err := decodeHex("DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF")
		if err == nil {
			t.Fatalf("expected err wrong length, got nil")
		}
	})
	t.Run("justRight", func(t *testing.T) {
		_, err := decodeHex("DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF")
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})
}

func TestAddGroupMembership(t *testing.T) {
	cases := []struct {
		name   string
		groups []Group
		err    error
	}{
		{
			name:   "emptyGroups",
			groups: []Group{},
			err:    nil,
		},
		{
			name: "happyPath",
			groups: []Group{
				{
					GroupID:   "ac97c9e2-346b-42a2-b6da-18bcb61a32fe",
					GroupName: "adminGroup",
				},
			},
			err: nil,
		},
	}
	a := NewTestAuth(t)
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			tkn := jwt.New(jwt.SigningMethodHS256)
			token, err := tkn.SignedString(a.SecretKey())
			if err != nil {
				t.Fatalf("unexpected error when signing token %v", err)
			}

			tokenWithGroups, err := a.addGroupMembership(token, test.groups)
			// okay this part kind of sucks bc we need to check errors and i
			// dont want to write a whole new test for things that should have
			// errors just to avoid this mess. errors.Is doesn't work either
			if (test.err == nil && err != nil) || (test.err != nil && err == nil) {
				t.Fatalf("expected %v but got %v", test.err, err)
			} else if test.err != nil && err != nil {
				if test.err.Error() != err.Error() {
					t.Fatalf("expected %v, but got %v", test.err, err)
				} else {
					return
				}
			}
			parsed, _, err := new(jwt.Parser).ParseUnverified(tokenWithGroups, jwt.MapClaims{})
			if err != nil {
				t.Fatalf("unexpected error parsing token %v", err)
			}

			claims := parsed.Claims.(jwt.MapClaims)
			groups, err := FromGob64(claims["molecula-idp-groups"].(string))
			if err != nil {
				t.Fatalf("unexpected error parsing groupString %v", err)
			}

			if !reflect.DeepEqual(groups, test.groups) {
				t.Fatalf("expected %v, got %v", test.groups, groups)
			}
		})
	}
}

func TestHandlers(t *testing.T) {
	a := NewTestAuth(t)
	t.Run("login", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/login", nil)
		w := httptest.NewRecorder()
		a.Login(w, req)
		resp := w.Result()
		if resp.StatusCode != http.StatusTemporaryRedirect {
			t.Fatalf("expected redirect, got %v", resp.StatusCode)
		}
		redirect := a.oAuthConfig.AuthCodeURL(a.oAuthConfig.Endpoint.AuthURL)
		if got, err := resp.Location(); err != nil || got.String() != redirect {
			t.Fatalf("expected %v, got %v", redirect, got.Path)
		}
	})
	t.Run("logout", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/logout", nil)
		w := httptest.NewRecorder()
		a.Logout(w, req)
		resp := w.Result()
		if resp.StatusCode != http.StatusTemporaryRedirect {
			t.Fatalf("expected redirect, got %v", resp.StatusCode)
		}
		redirect := fmt.Sprintf("%s?post_logout_redirect_uri=%s/", a.logoutEndpoint, a.fbURL)
		if got, err := resp.Location(); err != nil || got.String() != redirect {
			t.Fatalf("expected %v, got %v", redirect, got.Path)
		}
		for _, c := range resp.Cookies() {
			if c.Name == "molecula-chip" {
				if c.Value != "" {
					t.Fatalf("cookie not set to empty value!")
				}
				want := time.Unix(0, 0).Unix()
				got := c.Expires.Unix()
				if want != got {
					t.Fatalf("expected %v, got %v", want, got)
				}
				break
			}
		}
	})
}
