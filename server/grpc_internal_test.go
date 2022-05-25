package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v3/authn"
	"github.com/molecula/featurebase/v3/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestGetTokensFromMetadata(t *testing.T) {
	for name, test := range map[string]struct {
		access    string
		refresh   string
		setCookie bool
		md        metadata.MD
	}{
		"empty": {
			access:  "",
			refresh: "",
			md:      metadata.MD{},
		},
		"inTheCookieNoRefresh": {
			access:    "something",
			refresh:   "",
			md:        metadata.MD{},
			setCookie: true,
		},
		"inTheCookieYesRefresh": {
			access:    "something",
			refresh:   "somethingElse",
			md:        metadata.MD{},
			setCookie: true,
		},
		"otherCookies": {
			access:    "something",
			refresh:   "somethingElse",
			setCookie: true,
			md: metadata.MD{
				"cookie": []string{
					"okay=okay",
					"blah=blah",
				},
			},
		},
		"semiColonCookies": {
			access:    "something",
			refresh:   "somethingElse",
			setCookie: false,
			md: metadata.MD{
				"cookie": []string{
					fmt.Sprintf("%s=something; %s=somethingElse", authn.AccessCookieName, authn.RefreshCookieName),
				},
			},
		},
		"inTheHeaderNoRefresh": {
			access:  "something",
			refresh: "",
			md:      metadata.MD{"authorization": []string{"something"}},
		},
		"inTheHeaderYesRefresh": {
			access:  "something",
			refresh: "somethingElse",
			md: metadata.MD{
				"authorization":                          []string{"something"},
				strings.ToLower(authn.RefreshHeaderName): []string{"somethingElse"},
			},
		},
		"inTheHeaderYesRefreshCaps": {
			access:  "something",
			refresh: "somethingElse",
			md: metadata.MD{
				"authorization":         []string{"something"},
				authn.RefreshHeaderName: []string{"somethingElse"},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if test.setCookie {
				a := NewTestAuth(t)
				ctx := grpc.NewContextWithServerTransportStream(
					metadata.NewIncomingContext(context.TODO(),
						test.md,
					),
					NewServerTransportStream(),
				)
				err := a.SetGRPCMetadata(ctx, test.md, test.access, test.refresh)
				if err != nil {
					t.Errorf("unexpected error setting GRPC metadata: %v", err)
				}
			}
			accessGot, refreshGot := getTokensFromMetadata(test.md)
			if accessGot != test.access {
				t.Errorf("access: expected %v, got %v", test.access, accessGot)
			}
			if refreshGot != test.refresh {
				t.Errorf("refresh: expected %v, got %v", test.refresh, refreshGot)
			}
		})
	}
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
		logger.NopLogger,
		"http://localhost:10101/",
		Scopes,
		AuthorizeURL,
		TokenURL,
		GroupEndpointURL,
		LogoutURL,
		ClientID,
		ClientSecret,
		Key,
		[]string{},
	)
	if err != nil {
		t.Fatalf("building auth object%s", err)
	}
	return a
}
