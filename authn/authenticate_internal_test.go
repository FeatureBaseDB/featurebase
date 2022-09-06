package authn

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/featurebasedb/featurebase/v3/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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
		configuredIPs    = []string{}
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
		configuredIPs,
	)
	if err != nil {
		t.Fatalf("building auth object%s", err)
	}
	return a
}

func TestSetGRPCMetadata(t *testing.T) {
	a := NewTestAuth(t)
	for name, md := range map[string]metadata.MD{
		"empty":     {},
		"something": {"cookie": []string{a.accessCookieName + "=something"}},
		"somethingElse": {"cookie": []string{
			a.accessCookieName + "=something",
			a.refreshCookieName + "=something",
		}},
		"otherCookies": {"cookie": []string{a.accessCookieName + "=something", "blah=blah"}},
	} {
		t.Run(name, func(t *testing.T) {
			ogCookies, _ := md["cookie"]
			ctx := grpc.NewContextWithServerTransportStream(
				metadata.NewIncomingContext(context.TODO(),
					md,
				),
				NewServerTransportStream(),
			)
			md, ok := metadata.FromIncomingContext(ctx)
			if !ok {
				t.Fatalf("expected ok, got: %v", ok)
			}
			err := a.SetGRPCMetadata(ctx, md, "accesstoken!", "refreshtoken!")
			if err != nil {
				t.Fatalf("expected no errors, got: %v", err)
			}
			if err := grpc.SendHeader(ctx, md); err != nil {
				t.Fatalf("expected no errors, got: %v", err)
			}
			md, ok = metadata.FromIncomingContext(ctx)
			if !ok {
				t.Fatalf("expected ok, got: %v", ok)
			}
			c, ok := md["cookie"]
			if !ok {
				t.Fatalf("expected ok, got: %v", ok)
			}
			var accessCookie, refreshCookie string
			for _, cookie := range c {
				if strings.HasPrefix(cookie, a.accessCookieName) {
					accessCookie = cookie
				} else if strings.HasPrefix(cookie, a.refreshCookieName) {
					refreshCookie = cookie
				}
				if refreshCookie != "" && accessCookie != "" {
					break
				}
			}

			exp := a.accessCookieName + "=accesstoken!"
			if accessCookie != exp {
				t.Fatalf("expected '%v', got '%v'", exp, accessCookie)
			}
			exp = a.refreshCookieName + "=refreshtoken!"
			if refreshCookie != exp {
				t.Fatalf("expected '%v', got '%v'", exp, refreshCookie)
			}

			for _, cookie := range c {
				if strings.HasPrefix(cookie, a.accessCookieName) || strings.HasPrefix(cookie, a.refreshCookieName) {
					continue
				}
				found := false

				for _, ogCookie := range ogCookies {
					if cookie == ogCookie {
						found = true
						break
					}
				}
				if !found {
					t.Fatal("SetGRPCMetadata did not maintain the previous cookie list")
				}
			}
		})
	}
}

func TestAuth(t *testing.T) {
	a := NewTestAuth(t)
	t.Run("SetCookie", func(t *testing.T) {
		w := httptest.NewRecorder()
		err := a.SetCookie(w, "access", "refresh", time.Now().Add(time.Hour))
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
			[]string{},
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
}

func TestAuthenticate(t *testing.T) {
	cases := []struct {
		name         string
		uid          string
		uname        string
		exp          int64
		refresh      bool
		refreshToken string
		malformed    bool
		empty        bool
		groups       []Group
		err          error
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
			name:      "Malformed",
			malformed: true,
			err:       fmt.Errorf("parsing auth token: token contains an invalid number of segments"),
		},
		{
			name:  "Empty",
			empty: true,
			err:   fmt.Errorf("auth token is empty"),
		},
		{
			name:  "ExpiredTokenNoRefresh",
			uid:   "42",
			uname: "A. Token",
			groups: []Group{
				{
					GroupID:   "ac97c9e2-346b-42a2-b6da-18bcb61a32fe",
					GroupName: "adminGroup",
				},
			},
			exp: -17764800,
			err: fmt.Errorf("token is expired: refreshing token: 400 Bad Request"),
		},
		{
			name:  "ExpiredTokenYesRefresh",
			uid:   "42",
			uname: "A. Token",
			groups: []Group{
				{
					GroupID:   "ac97c9e2-346b-42a2-b6da-18bcb61a32fe",
					GroupName: "adminGroup",
				},
			},
			refresh:      true,
			refreshToken: "refreshToken",
			exp:          -17764800,
		},
		{
			name:  "ExpiredTokenYesRefreshButError",
			uid:   "42",
			uname: "A. Token",
			groups: []Group{
				{
					GroupID:   "ac97c9e2-346b-42a2-b6da-18bcb61a32fe",
					GroupName: "adminGroup",
				},
			},
			refresh:      true,
			refreshToken: "blah!!",
			exp:          -17764800,
			err:          fmt.Errorf("token is expired: refreshing token: 403 Forbidden"),
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			// setup the test
			a := NewTestAuth(t)
			token := ""
			var err error
			if !test.malformed && !test.empty {
				tkn := jwt.New(jwt.SigningMethodHS256)
				claims := tkn.Claims.(jwt.MapClaims)
				claims["oid"] = test.uid
				claims["name"] = test.uname
				if test.exp != 0 {
					claims["exp"] = float64(test.exp)
				}
				token, err = tkn.SignedString(a.SecretKey())
				if err != nil {
					t.Fatalf("unexpected error when signing token %v", err)
				}
			} else if !test.empty {
				token = "asdfasdfasdfasdF"
			}
			if len(test.groups) > 0 {
				a.groupsCache[token] = cachedGroups{time.Now(), test.groups}
			}
			if test.refresh {
				var srv *httptest.Server
				srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if err := r.ParseForm(); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					refresh := r.Form.Get("refresh_token")
					if refresh != test.refreshToken {
						t.Fatalf("refresh token not passed properly, expected %v, got %v", test.refreshToken, refresh)
						return
					}
					if refresh != "refreshToken" {
						http.Error(w, "bad token", http.StatusForbidden)
					}

					tkn := jwt.New(jwt.SigningMethodHS256)
					claims := tkn.Claims.(jwt.MapClaims)
					claims["oid"] = test.uid
					claims["name"] = test.uname
					expiry := float64(time.Now().Add(2 * time.Hour).Unix())
					claims["exp"] = expiry
					fresh, err := tkn.SignedString(a.SecretKey())
					if err != nil {
						t.Fatalf("unexpected error when signing token %v", err)
					}

					a.groupsCache[fresh] = cachedGroups{time.Now(), test.groups}
					fmt.Fprintf(w, `{"access_token": "`+fresh+`",  "refresh_token": "blah",  "token_type": "bearer",  "expires": `+strconv.FormatFloat(expiry, 'f', 0, 64)+` }`)
				}))
				defer srv.Close()
				a.oAuthConfig.Endpoint.TokenURL = srv.URL
			}

			// do the actual testing
			uinfo, err := a.Authenticate(token, test.refreshToken)
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

func TestAuthenticate_CleanCache(t *testing.T) {
	// this deserves its own test bc it has gross setup required
	t.Run("should clean", func(t *testing.T) {
		a := NewTestAuth(t)
		now := time.Now()
		a.groupsCache["oldy"] = cachedGroups{now.Add(-24 * time.Hour), []Group{}}
		a.groupsCache["goldy"] = cachedGroups{now.Add(-4 * time.Hour), []Group{}}
		a.lastCacheClean = now.Add(-45 * time.Minute)

		_, _ = a.Authenticate("this doesn't matter", "this doesn't matter?")
		if a.lastCacheClean.Sub(now) <= time.Nanosecond {
			t.Fatalf("cache should have been cleaned")
		}
		if _, ok := a.groupsCache["oldy"]; ok {
			t.Errorf("oldy should have been deleted")
		}
		if _, ok := a.groupsCache["goldy"]; !ok {
			t.Errorf("goldy should not have been deleted")
		}
	})
	t.Run("shouldn't clean", func(t *testing.T) {
		a := NewTestAuth(t)
		now := time.Now()
		a.groupsCache["oldy"] = cachedGroups{now.Add(-24 * time.Hour), []Group{}}
		a.groupsCache["goldy"] = cachedGroups{now.Add(-4 * time.Hour), []Group{}}
		a.lastCacheClean = now

		_, _ = a.Authenticate("this doesn't matter", "this doesn't matter?")
		if a.lastCacheClean.Sub(now) >= time.Nanosecond {
			t.Fatalf("cache should not have been cleaned")
		}
		if _, ok := a.groupsCache["oldy"]; !ok {
			t.Errorf("oldy should not have been deleted")
		}
		if _, ok := a.groupsCache["goldy"]; !ok {
			t.Errorf("goldy should not have been deleted")
		}
	})

}

func TestGetGroups(t *testing.T) {
	a := NewTestAuth(t)
	a.groupsCache = map[string]cachedGroups{
		"the world is changed": {
			cacheTime: time.Now(),
			groups: []Group{
				{
					GroupID:   "a han noston ned wilith",
					GroupName: "I smell it in the air",
				},
			},
		},
	}
	srvNext := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := json.Marshal(
			Groups{
				Groups: []Group{
					{
						GroupID:   "han mathon ne chae",
						GroupName: "I feel it in the earth",
					},
				},
			},
		)
		if err != nil {
			t.Fatalf("unexpected error marshalling groups response: %v", err)
		}
		fmt.Fprintf(w, "%s", body)
	}))
	defer srvNext.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := json.Marshal(
			Groups{
				NextLink: srvNext.URL,
				Groups: []Group{
					{
						GroupID:   "han mathon ne nen",
						GroupName: "i feel it in the water",
					},
				},
			},
		)
		if err != nil {
			t.Fatalf("unexpected error marshalling groups response: %v", err)
		}
		fmt.Fprintf(w, "%s", body)
	}))
	defer srv.Close()
	a.groupEndpoint = srv.URL

	for name, test := range map[string]struct {
		token  string
		groups []Group
	}{
		"InCache": {
			token: "the world is changed",
			groups: []Group{
				{
					GroupID:   "a han noston ned wilith",
					GroupName: "I smell it in the air",
				},
			},
		},
		"NotInCache": {
			token: "i smell it in the air",
			groups: []Group{
				{
					GroupID:   "han mathon ne nen",
					GroupName: "i feel it in the water",
				},
				{
					GroupID:   "han mathon ne chae",
					GroupName: "I feel it in the earth",
				},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := a.getGroups(test.token); err != nil || !reflect.DeepEqual(got, test.groups) {
				t.Errorf("expected %v, nil, got %v, %v", test.groups, got, err)
			}
		})
	}

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
		req.AddCookie(
			&http.Cookie{
				Name:     a.accessCookieName,
				Value:    "test",
				Path:     "/",
				Secure:   true,
				HttpOnly: true,
				SameSite: http.SameSiteStrictMode,
				Expires:  time.Unix(3000000, 0),
			},
		)

		req.AddCookie(
			&http.Cookie{
				Name:     a.refreshCookieName,
				Value:    "test",
				Path:     "/",
				Secure:   true,
				HttpOnly: true,
				SameSite: http.SameSiteStrictMode,
				Expires:  time.Unix(3000000, 0),
			},
		)

		a.groupsCache["test"] = cachedGroups{}
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
			if c.Name == a.accessCookieName || c.Name == a.refreshCookieName {
				if c.Value != "" {
					t.Fatalf("cookie not set to empty value!")
				}
				want := time.Unix(0, 0).Unix()
				got := c.Expires.Unix()
				if want != got {
					t.Fatalf("expected %v, got %v", want, got)
				}
			}
		}
		if _, ok := a.groupsCache["test"]; ok {
			t.Fatalf("groups not deleted!")
		}
	})
	t.Run("redirectGood", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/redirect", nil)
		w := httptest.NewRecorder()
		tkn := jwt.New(jwt.SigningMethodHS256)
		claims := tkn.Claims.(jwt.MapClaims)
		claims["oid"] = "user id"
		claims["name"] = "user name"
		expiresIn := 2 * time.Hour
		exp := time.Now().Add(expiresIn)
		expiry := float64(exp.Unix())
		claims["exp"] = expiry
		fresh, err := tkn.SignedString(a.SecretKey())
		if err != nil {
			t.Fatalf("unexpected error when signing token %v", err)
		}

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body := `{"access_token": "` + fresh + `", "refresh_token": "blah", "expires_in": "` + strconv.Itoa(int(expiresIn.Seconds())) + `"}`
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		}))
		a.oAuthConfig.Endpoint.TokenURL = srv.URL
		a.Redirect(w, req)
		resp := w.Result()
		if resp.StatusCode != http.StatusTemporaryRedirect {
			t.Fatalf("expected redirect, got %v", resp.StatusCode)
		}
		if got, err := resp.Location(); err != nil || got.String() != "/" {
			t.Fatalf("expected %v, got %v", "/", got.Path)
		}
		cookies := resp.Cookies()
		for _, c := range cookies {
			if c.Name == a.accessCookieName && c.Value != fresh {
				t.Fatalf("expected %v, got %v", exp, c.Value)
			} else if c.Name == a.refreshCookieName && c.Value != "blah" {
				t.Fatalf("expected %v, got %v", "blah", c.Value)
			}
		}
	})

	t.Run("redirectBad", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/redirect", nil)
		w := httptest.NewRecorder()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Server Error", http.StatusInternalServerError)
		}))
		a.oAuthConfig.Endpoint.TokenURL = srv.URL
		a.Redirect(w, req)
		resp := w.Result()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected BadRequest, got %v", resp.StatusCode)
		}
	})

}

// This type is used for mocking ServerTransportStreams in tests
type ServerTransportStream struct {
	md     metadata.MD
	method string
}

func NewServerTransportStream() *ServerTransportStream {
	return &ServerTransportStream{
		md:     metadata.MD{},
		method: "test",
	}
}

func (s *ServerTransportStream) Method() string {
	return s.method
}

func (s *ServerTransportStream) SetHeader(md metadata.MD) error {
	s.md = md
	return nil
}

func (s *ServerTransportStream) SendHeader(md metadata.MD) error {
	_ = md
	return nil
}

func (s *ServerTransportStream) SetTrailer(md metadata.MD) error {
	_ = md
	return nil
}

func TestCleanOAuthConfig(t *testing.T) {
	a := NewTestAuth(t)
	res := a.CleanOAuthConfig()
	assertEqual("", res.ClientSecret, t)
	assertEqual(a.oAuthConfig.ClientID, res.ClientID, t)
	assertEqual(a.oAuthConfig.RedirectURL, res.RedirectURL, t)
	assertEqual(a.oAuthConfig.Scopes, res.Scopes, t)
	assertEqual(a.oAuthConfig.Endpoint, res.Endpoint, t)
}

func assertEqual(exp, got interface{}, t *testing.T) {
	if !reflect.DeepEqual(exp, got) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestCheckAllowedNetworks(t *testing.T) {

	tests := []struct {
		requestIP     string
		configuredIPs []string
		isAdmin       bool
	}{
		{
			requestIP:     "10.0.0.1",
			configuredIPs: []string{"10.0.0.1"},
			isAdmin:       true,
		},
		{
			requestIP:     "10.0.0.3",
			configuredIPs: []string{"10.0.0.1", "10.0.0.2"},
			isAdmin:       false,
		},
		{
			requestIP:     "10.0.0.2",
			configuredIPs: []string{"10.0.0.1/30"},
			isAdmin:       true,
		},
		// it is possible for the client IP to have a port
		{
			requestIP:     "10.0.0.2:22",
			configuredIPs: []string{"10.0.0.1/30"},
			isAdmin:       true,
		},
		{
			requestIP:     "10.1.0.3",
			configuredIPs: []string{"10.0.0.1/32"},
			isAdmin:       false,
		},
		{
			requestIP:     "10.0.0.254",
			configuredIPs: []string{"10.0.0.1/24"},
			isAdmin:       true,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("network-%d", i), func(t *testing.T) {
			a := NewTestAuth(t)
			if err := a.convertIP(test.configuredIPs); err != nil {
				t.Fatalf("failed to convert IPs from strings to net.IP: %v", err)
			}
			got := a.CheckAllowedNetworks(test.requestIP)
			if got != test.isAdmin {
				t.Fatalf("expected %v, got %v", test.isAdmin, got)
			}
		})
	}
}

func TestConvertIP(t *testing.T) {

	tests := []struct {
		configuredIPs []string
		convertedIPs  []net.IPNet
	}{
		{
			configuredIPs: []string{"10.0.0.1"},
			convertedIPs: []net.IPNet{
				{IP: net.ParseIP("10.0.0.1"), Mask: net.CIDRMask(32, 32)},
			},
		},
		{
			configuredIPs: []string{"10.0.0.1/30"},
			convertedIPs: []net.IPNet{
				{IP: net.ParseIP("10.0.0.0"), Mask: net.CIDRMask(30, 32)},
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("network-%d", i), func(t *testing.T) {
			a := NewTestAuth(t)
			if err := a.convertIP(test.configuredIPs); err != nil {
				t.Fatalf("failed to convert IPs from strings to net.IP: %v", err)
			}

			if len(a.allowedNetworks) != len(test.convertedIPs) {
				t.Fatalf("expected len of %v networks, got %v", len(test.convertedIPs), len(a.allowedNetworks))
			}

			for i := range a.allowedNetworks {
				expected, got := test.convertedIPs[i], a.allowedNetworks[i]
				if got.IP.String() != expected.IP.String() {
					t.Fatalf("for IP, expected %v, got %v", expected.IP, got.IP)
				}
				if got.Mask.String() != expected.Mask.String() {
					t.Fatalf("for mask, expected %v, got %v", expected.Mask.String(), got.Mask.String())
				}
			}
		})
	}
}
