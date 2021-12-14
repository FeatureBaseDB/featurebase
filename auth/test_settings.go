package auth

import (
	"os"
	"time"

	"github.com/gorilla/securecookie"
	"github.com/molecula/featurebase/v2/logger"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/microsoft"
)

var (
	log           = logger.NewStandardLogger(os.Stderr)
	cookieName    = "molecula-session"
	refreshWithin = time.Second * time.Duration(15)
	hashKey       = securecookie.GenerateRandomKey(32)
	blockKey      = securecookie.GenerateRandomKey(32)
	secure        = securecookie.New(hashKey, blockKey)
	tenantID      = "4a137d66-d161-4ae4-b1e6-07e9920874b8"
	groupEndpoint = "https://graph.microsoft.com/v1.0/me/transitiveMemberOf/microsoft.graph.group?$count=true"
	OauthConfig   = &oauth2.Config{
		// TODO: MAKE REDIRECT URL DYNAMIC
		RedirectURL:  "http://localhost:10101/redirect",
		ClientID:     "e9088663-eb08-41d7-8f65-efb5f54bbb71",
		ClientSecret: "***REMOVED***",
		Scopes:       []string{"https://graph.microsoft.com/.default", "offline_access"},
		Endpoint:     microsoft.AzureADEndpoint(tenantID),
	}
)
