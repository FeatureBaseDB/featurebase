// Copyright 2021 Molecula Corp. All rights reserved.

// Package authn handles authentication
package authn

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/molecula/featurebase/v3/logger"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
)

// cachedGroups is used to hold groups and when they were last cached
type cachedGroups struct {
	cacheTime time.Time
	groups    []Group
}

// cacheToken is used to hold tokens and when they were added to the cache
type cachedToken struct {
	cacheTime time.Time
	token     *oauth2.Token
}

// UserInfo holds the information about the user from the token
type UserInfo struct {
	UserID   string    `json:"userid"`
	UserName string    `json:"username"`
	Groups   []Group   `json:"groups"`
	Expiry   time.Time `json:"expiry"`
	Token    string    `json:"token"`
}

// Group holds group information for an authenticated user
type Group struct {
	GroupID   string `json:"id"`
	GroupName string `json:"displayName"`
}

// Groups holds a slice of Group for marshalling from JSON
type Groups struct {
	NextLink string  `json:"@odata.nextLink"`
	Groups   []Group `json:"value"`
}

// Auth holds state, configuration, and utilities needed for authentication.
type Auth struct {
	logger         logger.Logger
	cookieName     string
	secretKey      []byte
	groupEndpoint  string
	logoutEndpoint string
	fbURL          string // fbURL is the domain featurebase is hosted on, used for post logout redirection
	oAuthConfig    *oauth2.Config
	cacheTTL       time.Duration           // cacheTTL is used to determine if a cached item should be refreshed or not
	tokenTTR       time.Duration           // tokenTTR (time to refresh) is used to determine if a token should be refreshed or not
	tokenCache     map[string]cachedToken  // tokenCache is a map of accessToken -> *oauth2.Token which we can use to refresh the tokens
	groupsCache    map[string]cachedGroups // groupsCache is a map of accessToken -> group memberships
	lastCacheClean time.Time               // last cache clean is the time that the cache was last cleaned
}

// NewAuth instantiates and returns a new Auth struct
func NewAuth(logger logger.Logger, url string, scopes []string, authURL, tokenURL, groupEndpoint, logout, clientID, clientSecret, secretKey string) (auth *Auth, err error) {
	auth = &Auth{
		logger:         logger,
		cookieName:     "molecula-chip",
		groupEndpoint:  groupEndpoint,
		logoutEndpoint: logout,
		fbURL:          url,
		oAuthConfig: &oauth2.Config{
			RedirectURL:  fmt.Sprintf("%s/redirect", url),
			ClientID:     clientID,
			ClientSecret: clientSecret,
			Scopes:       scopes,
			Endpoint: oauth2.Endpoint{
				AuthURL:  authURL,
				TokenURL: tokenURL,
			},
		},
		tokenCache:     map[string]cachedToken{},
		groupsCache:    map[string]cachedGroups{},
		cacheTTL:       10 * time.Minute,
		tokenTTR:       7 * time.Minute,
		lastCacheClean: time.Now(),
	}

	if auth.secretKey, err = decodeHex(secretKey); err != nil {
		return nil, errors.Wrap(err, "decoding secret key")
	}
	return auth, nil
}

// SecretKey is a convenient function to get the SecretKey from an Auth struct
func (a Auth) SecretKey() []byte {
	return a.secretKey
}

// Authenticate takes in a bearer token `bearer` and returns UserInfo from that token
// it is caller's responsibility to inform the user that the access token has been refreshed
func (a *Auth) Authenticate(ctx context.Context, bearer string) (*UserInfo, error) {
	// clean up the cache every 30 minutes or so
	if time.Now().Sub(a.lastCacheClean) >= 30*time.Minute {
		a.cleanCache()
	}

	if tkn, ok := a.tokenCache[bearer]; ok && (tkn.token.Expiry.Sub(time.Now()) <= a.tokenTTR || !tkn.token.Valid()) {
		// refresh the token
		resp, err := http.PostForm(a.oAuthConfig.Endpoint.TokenURL,
			url.Values{
				"grant_type":    {"refresh_token"},
				"refresh_token": {tkn.token.RefreshToken},
				"client_id":     {a.oAuthConfig.ClientID},
				"client_secret": {a.oAuthConfig.ClientSecret},
			},
		)
		if err != nil {
			return nil, errors.Wrap(err, "refreshing token")
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("refreshing token: %s", resp.Status)
		}
		defer resp.Body.Close()
		var t oauth2.Token
		if err := json.NewDecoder(resp.Body).Decode(&t); err != nil {
			return nil, errors.Wrap(err, "decoding refreshed token")
		}

		// update the cache
		delete(a.tokenCache, bearer)
		delete(a.groupsCache, bearer)
		bearer = t.AccessToken
		a.tokenCache[bearer] = cachedToken{time.Now(), &t}
	}

	if len(bearer) == 0 {
		return nil, fmt.Errorf("bearer token is empty")
	}

	// NOTE: we are using ParseUnverified here because the IDP validates the
	// token's signature when we get the user's groups, we just need to make
	// sure it's not expired and is well-formed
	token, _, err := new(jwt.Parser).ParseUnverified(bearer, &jwt.MapClaims{})
	// well-formed-ness check
	if token == nil || token.Claims == nil || err != nil {
		return nil, fmt.Errorf("parsing bearer token: %v", err)
	}

	claims := *token.Claims.(*jwt.MapClaims)

	// expiry check
	if exp, ok := claims["exp"].(string); ok {
		if expiry, err := strconv.ParseInt(exp, 10, 64); err != nil || expiry < time.Now().UTC().Unix() {
			return nil, fmt.Errorf("token is expired")
		}
	}

	userInfo := UserInfo{
		Token:  bearer,
		Groups: []Group{},
	}
	if uid, ok := claims["oid"].(string); ok {
		userInfo.UserID = uid
	}
	if name, ok := claims["name"].(string); ok {
		userInfo.UserName = name
	}

	if userInfo.Groups, err = a.getGroups(bearer); err != nil {
		return nil, errors.Wrap(err, "getting groups")
	}

	return &userInfo, nil
}

// cleanCache removes old items from our cache
func (a *Auth) cleanCache() {
	for bearer, tkn := range a.tokenCache {
		// if it's been more than 24 hours since the token was cached
		if time.Now().Sub(tkn.cacheTime) >= 24*time.Hour {
			// remove it from our cache
			delete(a.tokenCache, bearer)
		}
	}
	for bearer, tkn := range a.groupsCache {
		// if it's been more than 24 hours since the groups were cached
		if time.Now().Sub(tkn.cacheTime) >= 24*time.Hour {
			// remove it from our cache
			delete(a.groupsCache, bearer)
		}
	}
	a.lastCacheClean = time.Now()
}

// Login redirects a user to login to their configured oAuth authorize endpoint
func (a *Auth) Login(w http.ResponseWriter, r *http.Request) {
	authURL := a.oAuthConfig.AuthCodeURL(a.oAuthConfig.Endpoint.AuthURL)
	http.Redirect(w, r, authURL, http.StatusTemporaryRedirect)
}

// Logout clears out the user's cookie, removes the token from our cache, and
// redirects user to IdP's logout endpoint
func (a *Auth) Logout(w http.ResponseWriter, r *http.Request) {
	// remove the bearer token from a.tokenCache and a.groupsCache
	if bearer, err := r.Cookie(a.cookieName); err == nil {
		delete(a.tokenCache, bearer.Value)
		delete(a.groupsCache, bearer.Value)
	}
	// clear cookie
	http.SetCookie(w, &http.Cookie{
		Name:     a.cookieName,
		Value:    "",
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Expires:  time.Unix(0, 0),
	})

	http.Redirect(w, r, fmt.Sprintf("%s?post_logout_redirect_uri=%s/", a.logoutEndpoint, a.fbURL), http.StatusTemporaryRedirect)
}

// Redirect handles the oAuth /redirect endpoint. It gets an access token and
// returns it to the user in the form of a cookie
func (a *Auth) Redirect(w http.ResponseWriter, r *http.Request) {
	token, err := a.oAuthConfig.Exchange(r.Context(), r.FormValue("code"), oauth2.AccessTypeOffline)
	if err != nil {
		a.logger.Warnf("getting token from IdP: %+v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	a.tokenCache[token.AccessToken] = cachedToken{time.Now(), token}

	a.SetCookie(w, token.AccessToken, token.Expiry)
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

// getGroups gets the group membership for a given token from configured IdP
func (a *Auth) getGroups(token string) ([]Group, error) {
	var groups Groups

	gc, ok := a.groupsCache[token]
	if ok && (time.Now().Sub(gc.cacheTime) < a.cacheTTL) && len(gc.groups) > 0 {
		return gc.groups, nil
	}

	nextLink := a.groupEndpoint
	for nextLink != "" {
		req, err := http.NewRequest("GET", nextLink, nil)
		if err != nil {
			return nil, errors.Wrap(err, "creating new request to group endpoint")
		}

		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, errors.Wrap(err, "getting group membership info")
		}
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("getting group membership info: %s", response.Status)
		}

		var g Groups
		if err = json.NewDecoder(response.Body).Decode(&g); err != nil {
			return groups.Groups, errors.Wrap(err, "failed unmarshalling group membership response")
		}

		response.Body.Close()
		groups.Groups = append(groups.Groups, g.Groups...)
		nextLink = g.NextLink
	}

	if len(groups.Groups) == 0 {
		return nil, fmt.Errorf("no groups found")
	}

	a.groupsCache[token] = cachedGroups{
		cacheTime: time.Now(),
		groups:    groups.Groups,
	}
	return groups.Groups, nil
}

func (a *Auth) SetCookie(w http.ResponseWriter, token string, expiry time.Time) error {
	http.SetCookie(w, &http.Cookie{
		Name:     a.cookieName,
		Value:    token,
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Expires:  expiry,
	})
	return nil
}

func (a *Auth) SetGRPCMetadata(ctx context.Context, md metadata.MD, token string) error {
	cookies := []string{}
	if c, ok := md["cookie"]; ok {
		for _, cookie := range c {
			if strings.HasPrefix(cookie, a.cookieName) {
				cookie = a.cookieName + "=" + token
			}
			cookies = append(cookies, cookie)
		}
	}
	md["cookie"] = cookies
	return grpc.SetHeader(ctx, md)
}

func decodeHex(hexstr string) ([]byte, error) {
	data, err := hex.DecodeString(hexstr)
	if err != nil {
		return nil, errors.Wrap(err, "decoding hex string to byte slice")
	}
	if len(data) != 32 {
		return nil, fmt.Errorf("invalid key length")
	}
	return data, nil
}
