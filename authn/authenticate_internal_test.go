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
		ClientID         = "e9088663-eb08-41d7-8f65-efb5f54bbb71"
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
		ClientID,
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

	t.Run("SetCookie", func(t *testing.T) {
		w := httptest.NewRecorder()
		err := a.setCookie(w, &validCV)
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
	t.Run("GetEmptyCookie", func(t *testing.T) {
		c := a.getEmptyCookie()
		if c.Value != "" {
			t.Fatalf("expected empty cookie, got: %+v", c.Value)
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
			ClientID,
			ClientSecret,
			Key,
			ShortKey,
		)
		if err == nil || !strings.Contains(err.Error(), "decoding block key") {
			t.Fatalf("expected error decoding block key got: %v", err)
		}
	})
	t.Run("NewCookieValue-BadAccessToken", func(t *testing.T) {
		_, err := a.newCookieValue(&tokenAT)
		if err == nil || !strings.Contains(err.Error(), "jwt claims") {
			t.Fatalf("expected failure regarding jwt claims, got: %v", err)
		}

	})
	t.Run("CookieValue-NoAccessToken", func(t *testing.T) {
		_, err := a.newCookieValue(&tokenNoAT)
		if err == nil || !strings.Contains(err.Error(), "access token") {
			t.Fatalf("expected failure regarding access token, got: %v", err)
		}
	})

}
