package authn

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/molecula/featurebase/v3/logger"
	"golang.org/x/oauth2"
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
		err := a.SetCookie(w, "a cookie value", time.Now().Add(time.Hour))
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
	t.Run("SetGRPCMetadata", func(t *testing.T) {
		md := metadata.MD{
			"cookie": []string{a.cookieName + "=something"},
		}
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
		err := a.SetGRPCMetadata(ctx, md, "this is a token!")
		if err != nil {
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
		var cookie string
		for _, cookie = range c {
			if strings.HasPrefix(cookie, a.cookieName) {
				break
			}
		}
		if exp, got := a.cookieName+"=this is a token!", cookie; got != exp {
			t.Fatalf("expected '%v', got '%v'", exp, got)
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
}

func TestAuthenticate(t *testing.T) {
	cases := []struct {
		name         string
		uid          string
		uname        string
		exp          int64
		refresh      bool
		errOnRefresh bool
		malformed    bool
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
			err:       fmt.Errorf("parsing bearer token: token contains an invalid number of segments"),
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
			err: fmt.Errorf("token is expired"),
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
			refresh: true,
			exp:     -17764800,
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
			errOnRefresh: true,
			exp:          -17764800,
			err:          fmt.Errorf("refreshing token: 500 Internal Server Error"),
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			// setup the test
			a := NewTestAuth(t)
			token := ""
			var err error
			if !test.malformed {
				tkn := jwt.New(jwt.SigningMethodHS256)
				claims := tkn.Claims.(jwt.MapClaims)
				claims["oid"] = test.uid
				claims["name"] = test.uname
				if test.exp != 0 {
					claims["exp"] = strconv.Itoa(int(test.exp))
				}
				token, err = tkn.SignedString(a.SecretKey())
				if err != nil {
					t.Fatalf("unexpected error when signing token %v", err)
				}
			} else {
				token = "asdfasdfasdfasdF"
			}
			if len(test.groups) > 0 {
				a.groupsCache[token] = cachedGroups{time.Now(), test.groups}
			}
			if test.refresh {
				var srv *httptest.Server
				if !test.errOnRefresh {
					srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						tkn := jwt.New(jwt.SigningMethodHS256)
						claims := tkn.Claims.(jwt.MapClaims)
						claims["oid"] = test.uid
						claims["name"] = test.uname
						expiry := strconv.Itoa(int(time.Now().Add(2 * time.Hour).Unix()))
						claims["exp"] = expiry
						fresh, err := tkn.SignedString(a.SecretKey())
						if err != nil {
							t.Fatalf("unexpected error when signing token %v", err)
						}

						a.groupsCache[fresh] = cachedGroups{time.Now(), test.groups}
						fmt.Fprintf(w, `{"access_token": "`+fresh+`",  "refresh_token": "blah",  "token_type": "bearer",  "expires": `+expiry+` }`)
					}))
				} else {
					srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						http.Error(w, "bad", http.StatusInternalServerError)
					}))
				}
				defer srv.Close()
				a.oAuthConfig.Endpoint.TokenURL = srv.URL
				a.tokenCache[token] = cachedToken{
					time.Now(),
					&oauth2.Token{
						AccessToken:  token,
						RefreshToken: "blah",
						Expiry:       time.Unix(test.exp, 0),
					},
				}
			}

			// do the actual testing
			uinfo, err := a.Authenticate(context.TODO(), token)
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
		a.tokenCache["oldy"] = cachedToken{now.Add(-24 * time.Hour), &oauth2.Token{}}
		a.tokenCache["goldy"] = cachedToken{now.Add(-4 * time.Hour), &oauth2.Token{}}
		a.lastCacheClean = now.Add(-45 * time.Minute)

		_, _ = a.Authenticate(context.TODO(), "this doesn't matter")
		if a.lastCacheClean.Sub(now) <= time.Nanosecond {
			t.Fatalf("cache should have been cleaned")
		}
		if _, ok := a.groupsCache["oldy"]; ok {
			t.Errorf("oldy should have been deleted")
		}
		if _, ok := a.groupsCache["goldy"]; !ok {
			t.Errorf("goldy should not have been deleted")
		}
		if _, ok := a.tokenCache["oldy"]; ok {
			t.Errorf("oldy should have been deleted")
		}
		if _, ok := a.tokenCache["goldy"]; !ok {
			t.Errorf("goldy should not have been deleted")
		}
	})
	t.Run("shouldn't clean", func(t *testing.T) {
		a := NewTestAuth(t)
		now := time.Now()
		a.groupsCache["oldy"] = cachedGroups{now.Add(-24 * time.Hour), []Group{}}
		a.groupsCache["goldy"] = cachedGroups{now.Add(-4 * time.Hour), []Group{}}
		a.tokenCache["oldy"] = cachedToken{now.Add(-24 * time.Hour), &oauth2.Token{}}
		a.tokenCache["goldy"] = cachedToken{now.Add(-4 * time.Hour), &oauth2.Token{}}
		a.lastCacheClean = now

		_, _ = a.Authenticate(context.TODO(), "this doesn't matter")
		if a.lastCacheClean.Sub(now) >= time.Nanosecond {
			t.Fatalf("cache should not have been cleaned")
		}
		if _, ok := a.groupsCache["oldy"]; !ok {
			t.Errorf("oldy should not have been deleted")
		}
		if _, ok := a.groupsCache["goldy"]; !ok {
			t.Errorf("goldy should not have been deleted")
		}
		if _, ok := a.tokenCache["oldy"]; !ok {
			t.Errorf("oldy should not have been deleted")
		}
		if _, ok := a.tokenCache["goldy"]; !ok {
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
				Name:     a.cookieName,
				Value:    "test",
				Path:     "/",
				Secure:   true,
				HttpOnly: true,
				SameSite: http.SameSiteStrictMode,
				Expires:  time.Unix(3000000, 0),
			},
		)
		a.groupsCache["test"] = cachedGroups{}
		a.tokenCache["test"] = cachedToken{time.Now(), &oauth2.Token{}}
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
			if c.Name == a.cookieName {
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
		if _, ok := a.groupsCache["test"]; ok {
			t.Fatalf("groups not deleted!")
		}
		if _, ok := a.tokenCache["test"]; ok {
			t.Fatalf("token not deleted!")
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
		expiry := strconv.Itoa(int(exp.Unix()))
		claims["exp"] = expiry
		fresh, err := tkn.SignedString(a.SecretKey())
		if err != nil {
			t.Fatalf("unexpected error when signing token %v", err)
		}
		freshToken := oauth2.Token{
			AccessToken:  fresh,
			RefreshToken: "blah",
			Expiry:       exp,
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
		cachedToken := a.tokenCache[fresh].token
		if cachedToken.AccessToken != freshToken.AccessToken {
			t.Fatalf("expected %v, got %v", freshToken.AccessToken, cachedToken.AccessToken)
		}
		if cachedToken.RefreshToken != freshToken.RefreshToken {
			t.Fatalf("expected %v, got %v", freshToken.RefreshToken, cachedToken.RefreshToken)
		}
		if cachedToken.Expiry.Sub(freshToken.Expiry) > time.Second {
			t.Fatalf("expected %v, got %v", freshToken.Expiry, cachedToken.Expiry)
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
