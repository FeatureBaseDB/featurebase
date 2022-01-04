// Copyright 2021 Molecula Corp. All rights reserved.
package authn

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/gorilla/securecookie"
	"github.com/molecula/featurebase/v2/logger"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
)

// Auth holds state, configuration, and utilities needed for authentication.
type Auth struct {
	logger         logger.Logger
	cookieName     string
	refreshWithin  time.Duration
	hashKey        []byte
	blockKey       []byte
	secure         *securecookie.SecureCookie
	groupEndpoint  string
	logoutEndpoint string
	fbURL          string // fbURL is the domain FB is hosted on, used for post logout redirection
	oAuthConfig    *oauth2.Config
}

// NewAuth is a constructor that returns a new auth object
func NewAuth(logger logger.Logger, url string, scopes []string, authUrl, tokenUrl, groupEndpoint, logout, clientID, clientSecret, hashKey, blockKey string) (*Auth, error) {
	auth := &Auth{
		logger:         logger,
		cookieName:     "molecula-chip",
		refreshWithin:  15 * time.Minute,
		groupEndpoint:  groupEndpoint,
		logoutEndpoint: logout,
		fbURL:          url,
		oAuthConfig: &oauth2.Config{
			RedirectURL:  fmt.Sprintf("%s/redirect", url),
			ClientID:     clientID,
			ClientSecret: clientSecret,
			Scopes:       scopes,
			Endpoint: oauth2.Endpoint{
				AuthURL:  authUrl,
				TokenURL: tokenUrl,
			},
		},
	}
	var err error
	if auth.hashKey, err = decodeHex(hashKey); err != nil {
		return nil, errors.Wrap(err, "decoding hash key")
	}

	if auth.blockKey, err = decodeHex(blockKey); err != nil {
		return nil, errors.Wrap(err, "decoding block key")
	}

	auth.secure = securecookie.New(auth.hashKey, auth.blockKey)

	return auth, nil
}

type CookieValue struct {
	UserID          string
	UserName        string
	GroupMembership []Group
	Token           *oauth2.Token
}

type Groups struct {
	Groups []Group `json:"value"`
}

type Group struct {
	UserID    string
	GroupID   string `json:"id"`
	GroupName string `json:"displayName"`
}

type UserInfo struct {
	UserID   string `json:"userid"`
	UserName string `json:"username"`
}

// Authenticate reads and validates a cookie, redirects if invalid or missing, otherwise returns
// the group membership information stored in the cookie.
func (a *Auth) Authenticate(w http.ResponseWriter, r *http.Request) ([]Group, error) {
	cookie, err := a.readCookie(w, r)
	if err != nil {
		http.Redirect(w, r, "/signin", http.StatusTemporaryRedirect)
		return nil, err
	}
	if cookie.Token.Expiry.Before(time.Now().Add(a.refreshWithin)) {
		err = a.refreshToken(w, cookie)
		if err != nil {
			a.logger.Errorf("refreshing access token: ", err)
			if cookie.Token.Expiry.Before(time.Now()) {
				http.Redirect(w, r, "/signin", http.StatusTemporaryRedirect)
				return nil, err
			}
		}
	}
	if len(cookie.GroupMembership) == 0 {
		return nil, errors.New("user is not part of any groups in identity provider")
	}
	return cookie.GroupMembership, nil

}

// Login redirects user to the IdP authorize endpoint for auth code
func (a *Auth) Login(w http.ResponseWriter, r *http.Request) {
	authUrl := a.oAuthConfig.AuthCodeURL(a.oAuthConfig.Endpoint.AuthURL)
	http.Redirect(w, r, authUrl, http.StatusTemporaryRedirect)
}

// Logout clears out user cookie and redirects user to IdP's logout endpoint
func (a *Auth) Logout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, a.getEmptyCookie())
	redirect := fmt.Sprintf("%s?post_logout_redirect_uri=%s/", a.logoutEndpoint, a.fbURL)
	http.Redirect(w, r, redirect, http.StatusTemporaryRedirect)
}

// Redirect gets user information from IdP and sets a secure cookie
func (a *Auth) Redirect(w http.ResponseWriter, r *http.Request) {
	code := r.FormValue("code")
	token, err := a.getToken(r, code)
	if err != nil {
		a.logger.Warnf("getting token from IdP: %+v", err)
		http.Error(w, "Bad Request: 400", http.StatusBadRequest)
		return
	}

	cv, err := a.newCookieValue(token)
	if err != nil || cv == nil {
		a.logger.Warnf("creating cookie: %+v", err)
		http.Error(w, "Bad Request: 400", http.StatusBadRequest)
		return
	}

	a.setCookie(w, cv)
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

// GetUserInfo reads user's cookie and returns their username and userId
func (a *Auth) GetUserInfo(w http.ResponseWriter, r *http.Request) *UserInfo {
	var resp UserInfo
	cookie, err := a.readCookie(w, r)
	if err != nil {
		a.logger.Warnf("was not able to read cookie for req: %+v", r)
		return &resp
	}
	return &UserInfo{
		UserID:   cookie.UserID,
		UserName: cookie.UserName,
	}

}

// getToken exhanges authorization code for an oAuth2 token
func (a *Auth) getToken(r *http.Request, code string) (*oauth2.Token, error) {
	token, err := a.oAuthConfig.Exchange(r.Context(), code)
	if err != nil {
		return nil, errors.Wrap(err, "exchanging auth code for token")
	}
	return token, nil
}

// newCookieValue parses a jwt `token` and returns relevant information in a cookie value struct
func (a *Auth) newCookieValue(token *oauth2.Token) (*CookieValue, error) {
	if token == nil {
		return nil, errors.New("baking cookie due to nil token")
	}
	if token.AccessToken == "" {
		return nil, errors.New("no access token provided")
	}

	// We are using ParseUnverified here because we're using the OAuth2.0 authZ code flow
	// which assumes that the IdP gives good responses. This means that if the IdP is
	// insecure, then we are too. But that's the way OAuth works, unfortunately.
	// Also, we assume the jwt is not tampered with bc we communicate with the IdP over HTTPS only.
	accessParsed, _, err := new(jwt.Parser).ParseUnverified(token.AccessToken, jwt.MapClaims{})
	if accessParsed == nil || accessParsed.Claims == nil || err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("%v parsing jwt claims from access tokens", accessParsed))
	}
	claims := accessParsed.Claims.(jwt.MapClaims)

	groups, err := a.getGroupMembership(token)
	if err != nil {
		return nil, errors.Wrap(err, "getting group membership")
	}
	// not needed at this point in the logic and makes the encoded cookie too large
	token.AccessToken = ""
	return &CookieValue{
		UserID:          claims["oid"].(string),
		UserName:        claims["name"].(string),
		GroupMembership: groups.Groups,
		Token:           token,
	}, nil
}

// getGroupMembership uses a oauth2 token to retrieve group membership information from IdP
func (a *Auth) getGroupMembership(token *oauth2.Token) (Groups, error) {
	var groups Groups
	req, err := http.NewRequest("GET", a.groupEndpoint, nil)
	if err != nil {
		return groups, errors.Wrap(err, "creating new request to group endpoint")
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token.AccessToken))
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return groups, errors.Wrap(err, "getting group membership info")
	}

	defer response.Body.Close()
	rawGroups, err := io.ReadAll(response.Body)
	if err != nil {
		return groups, errors.Wrap(err, "failed reading group membership response")
	}

	if err = json.Unmarshal(rawGroups, &groups); err != nil {
		return groups, errors.Wrap(err, "failed unmarshalling group membership response")
	}

	return groups, nil
}

// readCookie decodes an encrypted and signed cookie and returns the contained info
func (a *Auth) readCookie(w http.ResponseWriter, r *http.Request) (*CookieValue, error) {
	cookie, err := r.Cookie(a.cookieName)
	if err != nil {
		return nil, errors.Wrap(err, "cookie not found")
	}

	var value CookieValue
	err = a.secure.Decode(a.cookieName, cookie.Value, &value)
	if err != nil {
		http.SetCookie(w, a.getEmptyCookie())
		return nil, errors.Wrap(err, "decoding cookie")
	}

	return &value, nil
}

func (a *Auth) setCookie(w http.ResponseWriter, cookie *CookieValue) error {
	encoded, err := a.secure.Encode(a.cookieName, cookie)
	if err != nil {
		return errors.Wrap(err, "encoding CookieValue")

	}
	http.SetCookie(w, &http.Cookie{
		Name:     a.cookieName,
		Value:    encoded,
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Expires:  cookie.Token.Expiry,
	})
	return nil
}

func (a *Auth) refreshToken(w http.ResponseWriter, cookie *CookieValue) error {
	if cookie.Token.RefreshToken == "" {
		return errors.New("no refresh token found, check auth scopes to see if refresh tokens are being provided by your IdP.")
	}
	tokenSource := a.oAuthConfig.TokenSource(context.Background(), cookie.Token)
	newToken, err := tokenSource.Token()
	if err != nil {
		return errors.Wrap(err, "refreshing token")
	}

	if newToken.Expiry != cookie.Token.Expiry {
		cv, err := a.newCookieValue(newToken)
		if err != nil {
			return errors.Wrap(err, "creating cookie value from token")
		}

		a.setCookie(w, cv)
	}

	return nil
}

func decodeHex(hexstr string) ([]byte, error) {
	data, err := hex.DecodeString(hexstr)
	if err != nil {
		return nil, errors.Wrap(err, "decoding hex string to byte slice")
	}
	if len(data) != 32 {
		return nil, errors.Wrap(err, "invalid key length")
	}
	return data, nil
}

func (a *Auth) getEmptyCookie() *http.Cookie {
	return &http.Cookie{
		Name:     a.cookieName,
		Value:    "",
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	}
}
