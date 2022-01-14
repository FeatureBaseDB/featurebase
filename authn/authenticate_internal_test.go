package authn

import (
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2/logger"
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
	)
	if err != nil {
		t.Errorf("building auth object%s", err)
	}

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
			Scopes,
			AuthorizeURL,
			TokenURL,
			GroupEndpointURL,
			LogoutURL,
			ClientID,
			ClientSecret,
			ShortKey,
		)
		if err == nil || !strings.Contains(err.Error(), "decoding secret key") {
			t.Fatalf("expected error decoding secret key got: %v", err)
		}
	})
}
