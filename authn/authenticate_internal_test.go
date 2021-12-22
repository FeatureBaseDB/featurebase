package authn

import (
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2/logger"
	"golang.org/x/oauth2"
)

func TestAuth(t *testing.T) {
	var (
		ClientId         = "e9088663-eb08-41d7-8f65-efb5f54bbb71"
		ClientSecret     = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
		AuthorizeURL     = "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/authorize"
		TokenURL         = "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/token"
		GroupEndpointURL = "https://graph.microsoft.com/v1.0/me/transitiveMemberOf/microsoft.graph.group?$count=true"
		LogoutURL        = "https://login.microsoftonline.com/common/oauth2/v2.0/logout"
		Scopes           = []string{"https://graph.microsoft.com/.default", "offline_access"}
		Key              = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
		ShortKey         = "DEADBEEFD"
	)

	a, err := NewAuth(
		logger.NewStandardLogger(os.Stdout),
		"http://localhost:10101/",
		Scopes,
		AuthorizeURL,
		TokenURL,
		GroupEndpointURL,
		LogoutURL,
		ClientId,
		ClientSecret,
		Key,
		Key,
	)
	if err != nil {
		t.Errorf("building auth object%s", err)
	}
	tokenNoAT := oauth2.Token{
		TokenType:    "Bearer",
		RefreshToken: "abcdef",
		Expiry:       time.Now().Add(time.Hour),
	}
	tokenAT := oauth2.Token{
		TokenType:    "Bearer",
		RefreshToken: "abcdef",
		AccessToken:  "aasdf",
		Expiry:       time.Now().Add(time.Hour),
	}
	// expiredToken := oauth2.Token{
	// 	TokenType:    "Bearer",
	// 	RefreshToken: "abcdef",
	// 	Expiry:       time.Now(),
	// }
	grp := Group{
		UserID:    "snowstorm",
		GroupID:   "abcd123-A",
		GroupName: "Romantic Painters",
	}
	validCV := CookieValue{
		UserID:          "snowstorm",
		UserName:        "J.M.W. Turner",
		GroupMembership: []Group{grp},
		Token:           &tokenAT,
	}

	// t.Run("Login", func(t *testing.T) {

	// 	r := httptest.NewRequest(gohttp.MethodGet, "/login", nil)
	// 	w := httptest.NewRecorder()
	// 	a.Login(w, r)
	// 	res := w.Result()
	// 	defer res.Body.Close()
	// 	data, err := ioutil.ReadAll(res.Body)
	// 	if err != nil {
	// 		t.Errorf("expected no errors reading response, got: %+v", err)
	// 	}

	// 	// redir := "http://localhost:10101/"

	// 	// redirecturl := fmt.Sprintf("%s?client_id=%s&redirect_uri=%s&response_type=%s&scope=%s+%s&state=%s", settings.Auth.AuthorizeURL, settings.Auth.ClientId, redir, "code", settings.Auth.Scopes[0], settings.Auth.Scopes[1], settings.Auth.AuthorizeURL)

	// 	if res.Status != "307 Temporary Redirect" {
	// 		t.Errorf("expected status code 307 Temporary Redirect, got: %v", err)
	// 	}

	// 	if !strings.Contains(string(data), settings.Auth.AuthorizeURL) {
	// 		t.Errorf("expected url: %v, %v", settings.Auth.AuthorizeURL, string(data))
	// 	}

	// })
	// t.Run("Logout", func(t *testing.T) {
	// 	r := httptest.NewRequest(gohttp.MethodGet, "/login", nil)
	// 	w := httptest.NewRecorder()
	// 	newCookie := &gohttp.Cookie{
	// 		Name:     "brood",
	// 		Value:    "lacrimosa",
	// 		Path:     "/",
	// 		Secure:   true,
	// 		HttpOnly: true,
	// 		Expires:  time.Now().Add(8000),
	// 	}
	// 	gohttp.SetCookie(w, newCookie)

	// 	a.Login(w, r)
	// 	res := w.Result()
	// 	defer res.Body.Close()
	// 	data, err := ioutil.ReadAll(res.Body)
	// 	if err != nil {
	// 		t.Errorf("expected no errors reading response, got: %+v", err)
	// 	}

	// 	if res.Status != "307 Temporary Redirect" {
	// 		t.Errorf("expected status code 307 Temporary Redirect, got: %v", err)
	// 	}

	// 	if !strings.Contains(string(data), settings.Auth.AuthorizeURL) {
	// 		t.Errorf("expected url: %v, %v", settings.Auth.AuthorizeURL, string(data))
	// 	}

	// })

	// t.Run("Logout", func(t *testing.T) {
	// 	r := httptest.NewRequest(gohttp.MethodGet, "/logout", nil)
	// 	w := httptest.NewRecorder()
	// 	a.Logout(w, r)
	// })
	// t.Run("Authenticate", func(t *testing.T) {
	// 	r := httptest.NewRequest(gohttp.MethodGet, "/authenticate", nil)
	// 	w := httptest.NewRecorder()
	// 	a.Authenticate(w, r)
	// })
	// // t.Run("Redirect", func(t *testing.T) {
	// // 	r := httptest.NewRequest(gohttp.MethodGet, "/login", nil)
	// // 	w := httptest.NewRecorder()
	// // 	a.Redirect(w, r)
	// // })
	t.Run("SetCookie", func(t *testing.T) {
		w := httptest.NewRecorder()
		err := a.setCookie(w, &validCV)
		if err != nil {
			t.Errorf("expected no errors, got: %v", err)
		}

		if w.Result().Cookies()[0].Value == "" {
			t.Errorf("expected some value, got: %+v", w.Result().Cookies()[0].Value)
		}
		if w.Result().Cookies()[0].Path != "/" {
			t.Errorf("expected path to be /, got: %+v", w.Result().Cookies()[0].Path)
		}
	})
	t.Run("GetEmptyCookie", func(t *testing.T) {
		c := a.getEmptyCookie()
		if c.Value != "" {
			t.Errorf("expected empty cookie, got: %+v", c.Value)
		}
	})
	t.Run("KeyLength", func(t *testing.T) {
		_, err := NewAuth(
			logger.NewStandardLogger(os.Stdout),
			"http://localhost:10101/",
			Scopes,
			AuthorizeURL,
			TokenURL,
			GroupEndpointURL,
			LogoutURL,
			ClientId,
			ClientSecret,
			Key,
			ShortKey,
		)
		if err == nil || !strings.Contains(err.Error(), "decoding block key") {
			t.Errorf("expected error decoding block key got: %v", err)
		}
	})
	t.Run("NewCookieValue-BadAccessToken", func(t *testing.T) {
		_, err := a.newCookieValue(&tokenAT)
		if err == nil || !strings.Contains(err.Error(), "jwt claims") {
			t.Errorf("expected failure regarding jwt claims, got: %v", err)
		}

	})
	t.Run("CookieValue-NoAccessToken", func(t *testing.T) {
		_, err := a.newCookieValue(&tokenNoAT)
		if err == nil || !strings.Contains(err.Error(), "access token") {
			t.Errorf("expected failure regarding access token, got: %v", err)
		}
	})

}
