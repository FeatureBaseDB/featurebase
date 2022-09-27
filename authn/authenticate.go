// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0

// Package authn handles authentication
package authn

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
)

type AuthContextKey string

const (
	// AccessCookieName is the name of the cookie that holds the access token.
	AccessCookieName = "molecula-chip"

	// RefreshCookieName is the name of the cookie that holds the refresh token.
	RefreshCookieName = "refresh-molecula-chip"

	// RefreshHeaderName is the name of the header that holds the refresh token.
	RefreshHeaderName = "X-Molecula-Refresh-Token"

	// ContextValueAccessToken is the key used to set AccessTokens in a ctx.
	ContextValueAccessToken = AuthContextKey("Access")

	// ContextValueRefreshToken is the key used to set RefreshTokens in a ctx.
	ContextValueRefreshToken = AuthContextKey("Refresh")
)

// cachedGroups is used to hold groups and when they were last cached
type cachedGroups struct {
	cacheTime time.Time
	groups    []Group
}

// UserInfo holds the information about the user from the token
type UserInfo struct {
	UserID       string    `json:"userid"`
	UserName     string    `json:"username"`
	Groups       []Group   `json:"groups"`
	Expiry       time.Time `json:"expiry"`
	Token        string    `json:"token"`
	RefreshToken string    `json:"refreshtoken"`
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
	logger            logger.Logger
	accessCookieName  string
	refreshCookieName string
	secretKey         []byte
	groupEndpoint     string
	logoutEndpoint    string
	fbURL             string // fbURL is the domain featurebase is hosted on, used for post logout redirection
	oAuthConfig       *oauth2.Config
	cacheTTL          time.Duration           // cacheTTL is used to determine if a cached item should be refreshed or not
	groupsCache       map[string]cachedGroups // groupsCache is a map of accessToken -> group memberships
	lastCacheClean    time.Time               // last cache clean is the time that the cache was last cleaned
	allowedNetworks   []net.IPNet             // list of allowed networks for ingest
}

// NewAuth instantiates and returns a new Auth struct
func NewAuth(logger logger.Logger, url string, scopes []string, authURL, tokenURL, groupEndpoint, logout, clientID, clientSecret, secretKey string, configuredIPs []string) (auth *Auth, err error) {
	auth = &Auth{
		logger:            logger,
		accessCookieName:  AccessCookieName,
		refreshCookieName: RefreshCookieName,
		groupEndpoint:     groupEndpoint,
		logoutEndpoint:    logout,
		fbURL:             url,
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
		groupsCache:    map[string]cachedGroups{},
		cacheTTL:       10 * time.Minute,
		lastCacheClean: time.Now(),
	}

	if auth.secretKey, err = decodeHex(secretKey); err != nil {
		return nil, errors.Wrap(err, "decoding secret key")
	}

	// convert IPs and add them to allowed networks
	err = auth.convertIP(configuredIPs)
	if err != nil {
		return nil, err
	}

	return auth, nil
}

// CleanOAuthConfig returns a's oauthConfig without the client secret
func (a Auth) CleanOAuthConfig() oauth2.Config {
	b := *a.oAuthConfig
	b.ClientSecret = ""
	return b
}

// SecretKey is a convenient function to get the SecretKey from an Auth struct
func (a Auth) SecretKey() []byte {
	return a.secretKey
}

// refreshToken refreshes a given access/refresh token pair
func (a *Auth) refreshToken(access, refresh string) (string, string, error) {
	resp, err := http.PostForm(a.oAuthConfig.Endpoint.TokenURL,
		url.Values{
			"grant_type":    {"refresh_token"},
			"refresh_token": {refresh},
			"client_id":     {a.oAuthConfig.ClientID},
			"client_secret": {a.oAuthConfig.ClientSecret},
		},
	)

	if err != nil {
		return "", "", errors.Wrap(err, "refreshing token")
	}

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("refreshing token: %s", resp.Status)
	}

	defer resp.Body.Close()

	var t oauth2.Token
	if err := json.NewDecoder(resp.Body).Decode(&t); err != nil {
		return "", "", errors.Wrap(err, "decoding refreshed token")
	}

	// remove the old groups from the groups cache
	delete(a.groupsCache, access)

	return t.AccessToken, t.RefreshToken, nil
}

// Authenticate takes in a auth token `access` and returns UserInfo from that token
// it is caller's responsibility to inform the user that the access token has been refreshed
func (a *Auth) Authenticate(access, refresh string) (*UserInfo, error) {
	// clean up the cache every 30 minutes or so
	if time.Since(a.lastCacheClean) >= 30*time.Minute {
		a.cleanCache()
	}

	if len(access) == 0 {
		return nil, fmt.Errorf("auth token is empty")
	}

	// NOTE: we are using ParseUnverified here because the IDP validates the
	// token's signature when we get the user's groups, we just need to make
	// sure it's not expired and is well-formed
	token, _, err := new(jwt.Parser).ParseUnverified(access, &jwt.MapClaims{})
	// well-formed-ness check
	if token == nil || token.Claims == nil || err != nil {
		return nil, fmt.Errorf("parsing auth token: %v", err)
	}

	claims := *token.Claims.(*jwt.MapClaims)

	// expiry check
	if exp, ok := claims["exp"]; ok {
		var expiry int64
		switch v := exp.(type) {
		case string:
			expiry, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parsing exp string: %v", err)
			}
		case float64:
			expiry = int64(v)
		case int64:
			expiry = v
		}

		if expiry < time.Now().UTC().Unix() {
			access, refresh, err = a.refreshToken(access, refresh)
			if err != nil {
				return nil, fmt.Errorf("token is expired: %w", err)
			}
		}
	}

	userInfo := UserInfo{
		Token:        access,
		RefreshToken: refresh,
		Groups:       []Group{},
	}

	if uid, ok := claims["oid"].(string); ok {
		userInfo.UserID = uid
	}
	if name, ok := claims["name"].(string); ok {
		userInfo.UserName = name
	}

	if userInfo.Groups, err = a.getGroups(access); err != nil {
		return nil, errors.Wrap(err, "getting groups")
	}

	return &userInfo, nil
}

// cleanCache removes old items from our cache
func (a *Auth) cleanCache() {
	for access, tkn := range a.groupsCache {
		// if it's been more than 24 hours since the groups were cached
		if time.Since(tkn.cacheTime) >= 24*time.Hour {
			// remove it from our cache
			delete(a.groupsCache, access)
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
	// remove the access token from a.groupsCache
	if access, err := r.Cookie(a.accessCookieName); err == nil {
		delete(a.groupsCache, access.Value)
	}
	// clear cookie
	http.SetCookie(w, &http.Cookie{
		Name:     a.accessCookieName,
		Value:    "",
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Expires:  time.Unix(0, 0),
	})
	http.SetCookie(w, &http.Cookie{
		Name:     a.refreshCookieName,
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

	a.SetCookie(w, token.AccessToken, token.RefreshToken, token.Expiry)
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

// getGroups gets the group membership for a given token from configured IdP
func (a *Auth) getGroups(token string) ([]Group, error) {
	var groups Groups

	gc, ok := a.groupsCache[token]
	if ok && (time.Since(gc.cacheTime) < a.cacheTTL) && len(gc.groups) > 0 {
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

func (a *Auth) SetCookie(w http.ResponseWriter, access, refresh string, expiry time.Time) error {
	http.SetCookie(w, &http.Cookie{
		Name:     a.refreshCookieName,
		Value:    refresh,
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Expires:  expiry,
	})

	http.SetCookie(w, &http.Cookie{
		Name:     a.accessCookieName,
		Value:    access,
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Expires:  expiry,
	})
	return nil
}

func (a *Auth) SetGRPCMetadata(ctx context.Context, md metadata.MD, access, refresh string) error {
	mCookies := map[string]string{}
	if c, ok := md["cookie"]; ok {
		for _, cookie := range c {
			name, val := parseCookie(cookie)
			mCookies[name] = val
		}
	}

	mCookies[a.accessCookieName] = access
	mCookies[a.refreshCookieName] = refresh

	cookies := []string{}
	for name, val := range mCookies {
		cookies = append(cookies, name+"="+val)
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

func (a *Auth) convertIP(configuredIPs []string) error {
	sz := len(configuredIPs)
	nets := make([]net.IPNet, sz)
	for i, ip := range configuredIPs {
		// skip empty strings
		if ip == "" {
			sz--
			continue
		}
		// for IPs passed without a subnet, append /32 to only allow 1 IP
		// this step is needed because ParseCIDR method assumes a CIDR address
		if !strings.Contains(ip, "/") {
			ip = ip + "/32"
		}
		_, subnet, err := net.ParseCIDR(ip)
		if err != nil {
			return errors.Wrapf(err, "parsing CIDR for %v", ip)
		}
		nets[i] = *subnet
	}
	a.allowedNetworks = nets[:sz]
	return nil
}

// if IP is in allowed networks, then return true to grant admin permissions
func (a *Auth) CheckAllowedNetworks(clientIP string) bool {
	clientIP = strings.Split(clientIP, ":")[0]
	convertedIP := net.ParseIP(clientIP)
	for _, network := range a.allowedNetworks {
		if network.Contains(convertedIP) {
			return true
		}
	}
	return false
}

func parseCookie(cookie string) (name, data string) {
	vals := strings.Split(cookie, "=")
	if len(vals) == 0 {
		vals = []string{"", ""}
	} else if len(vals) < 2 {
		vals = append(vals, "")
	}
	return vals[0], vals[1]
}
