package authn_test

import (
	"io/ioutil"
	gohttp "net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v2/authn"
	"github.com/molecula/featurebase/v2/logger"
	"github.com/molecula/featurebase/v2/server"
)

func TestAuth(t *testing.T) {

	settings := server.Config{}
	settings.Auth.Enable = true
	settings.Auth.ClientId = "e9088663-eb08-41d7-8f65-efb5f54bbb71"
	settings.Auth.ClientSecret = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
	settings.Auth.AuthorizeURL = "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/authorize"
	settings.Auth.TokenURL = "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/token"
	settings.Auth.GroupEndpointURL = "https://graph.microsoft.com/v1.0/me/transitiveMemberOf/microsoft.graph.group?$count=true"
	settings.Auth.Scopes = []string{"https://graph.microsoft.com/.default", "offline_access"}
	settings.Auth.HashKey = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
	settings.Auth.BlockKey = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"

	a, err := authn.NewAuth(
		logger.NewStandardLogger(os.Stdout),
		"http://localhost:10101/",
		[]string{"https://graph.microsoft.com/.default", "offline_access"},
		"https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/authorize",
		"https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/token",
		"https://graph.microsoft.com/v1.0/me/transitiveMemberOf/microsoft.graph.group?$count=true",
		"e9088663-eb08-41d7-8f65-efb5f54bbb71",
		"DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF",
		"DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF",
		"DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF",
	)
	if err != nil {
		t.Errorf("building auth object%s", err)
	}

	t.Run("Login", func(t *testing.T) {

		r := httptest.NewRequest(gohttp.MethodGet, "/login", nil)
		w := httptest.NewRecorder()
		a.Login(w, r)
		res := w.Result()
		defer res.Body.Close()
		data, err := ioutil.ReadAll(res.Body)
		if err != nil {
			t.Errorf("expected no errors reading response, got: %+v", err)
		}

		// redir := "http://localhost:10101/"

		// redirecturl := fmt.Sprintf("%s?client_id=%s&redirect_uri=%s&response_type=%s&scope=%s+%s&state=%s", settings.Auth.AuthorizeURL, settings.Auth.ClientId, redir, "code", settings.Auth.Scopes[0], settings.Auth.Scopes[1], settings.Auth.AuthorizeURL)

		if res.Status != "307 Temporary Redirect" {
			t.Errorf("expected status code 307 Temporary Redirect, got: %v", err)
		}

		if !strings.Contains(string(data), settings.Auth.AuthorizeURL) {
			t.Errorf("expected url: %v, %v", settings.Auth.AuthorizeURL, string(data))
		}

	})
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

	t.Run("Logout", func(t *testing.T) {
		r := httptest.NewRequest(gohttp.MethodGet, "/login", nil)
		w := httptest.NewRecorder()
		a.Logout(w, r)
	})
	t.Run("Authenticate", func(t *testing.T) {
		r := httptest.NewRequest(gohttp.MethodGet, "/login", nil)
		w := httptest.NewRecorder()
		a.Authenticate(w, r)
	})
	// t.Run("Redirect", func(t *testing.T) {
	// 	r := httptest.NewRequest(gohttp.MethodGet, "/login", nil)
	// 	w := httptest.NewRecorder()
	// 	a.Redirect(w, r)
	// })
	t.Run("GetUserInfo", func(t *testing.T) {
		r := httptest.NewRequest(gohttp.MethodGet, "/login", nil)
		a.GetUserInfo(r)
	})

}
