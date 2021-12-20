// Copyright 2021 Molecula Corp. All rights reserved.
package authn

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/gorilla/securecookie"
	"github.com/molecula/featurebase/v2/logger"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
)

type Auth struct {
	logger         logger.Logger
	cookieName     string
	refreshWithin  time.Duration
	hashKey        []byte
	blockKey       []byte
	secure         *securecookie.SecureCookie
	groupEndpoint  string
	logoutEndpoint string
	fbURL          string
	oAuthConfig    *oauth2.Config
}

func NewAuth(logger logger.Logger, url string, scopes []string, authUrl, tokenUrl, groupEndpoint, clientID, clientSecret, hashKey, blockKey string) (*Auth, error) {
	auth := &Auth{
		logger:         logger,
		cookieName:     "molecula-chip",
		refreshWithin:  time.Minute * time.Duration(15),
		groupEndpoint:  groupEndpoint,
		logoutEndpoint: "https://login.microsoftonline.com/common/oauth2/v2.0/logout",
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

func (a *Auth) Authenticate(w http.ResponseWriter, r *http.Request) ([]Group, error) {
	cookie, err := a.readCookie(r)
	if err != nil {
		http.Redirect(w, r, "/signin", http.StatusTemporaryRedirect)
		return nil, err
	}
	if cookie.Token.Expiry.Before(time.Now().Add(a.refreshWithin)) {
		err = a.refreshToken(w, cookie)
		if err != nil {
			//log error
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

func (a *Auth) Login(w http.ResponseWriter, r *http.Request) {
	authUrl := a.oAuthConfig.AuthCodeURL(a.oAuthConfig.Endpoint.AuthURL)
	http.Redirect(w, r, authUrl, http.StatusTemporaryRedirect)
}

func (a *Auth) Logout(w http.ResponseWriter, r *http.Request) {
	newCookie := &http.Cookie{
		Name:     a.cookieName,
		Value:    "",
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
	}
	http.SetCookie(w, newCookie)
	redirect := fmt.Sprintf("%s?post_logout_redirect_uri=%s/", a.logoutEndpoint, a.fbURL)
	http.Redirect(w, r, redirect, http.StatusTemporaryRedirect)
}

// Gets user information from dP and sets a secure cookie
func (a *Auth) Redirect(w http.ResponseWriter, r *http.Request) {
	code := r.FormValue("code")
	token, err := a.getToken(code)
	if err != nil {
		errors.Wrap(err, "getting token")
		http.Redirect(w, r, "/login", http.StatusUnauthorized)
	}

	cv, err := a.newCookieValue(token)
	if err != nil {
		http.Error(w, "authenticating", http.StatusBadRequest)
	}

	a.setCookie(w, cv)
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

func (a *Auth) GetUserInfo(r *http.Request) *UserInfo {
	var resp UserInfo
	cookie, err := a.readCookie(r)
	if err != nil {
		//add logging
		return &resp
	}
	resp.UserID = cookie.UserID
	resp.UserName = cookie.UserName
	return &resp

}

func (a *Auth) getToken(code string) (*oauth2.Token, error) {
	token, err := a.oAuthConfig.Exchange(context.Background(), code)
	if err != nil {
		return nil, errors.Wrap(err, "exchanging auth code for token")
	}
	return token, nil
}

func (a *Auth) newCookieValue(token *oauth2.Token) (*CookieValue, error) {
	if token == nil {
		return nil, errors.New("baking cookie due to nil token")
	}
	accessParsed, err := jwt.Parse(token.AccessToken, nil)
	if token == nil {
		fmt.Println(errors.Wrap(err, "parsing jwt claims from access tokens"))
	}
	claims := accessParsed.Claims.(jwt.MapClaims)

	groups, err := a.getGroupMembership(token)
	if err != nil {
		fmt.Println(errors.Wrap(err, "getting group memebership"))
	}
	// not needed at this point in the logic and makes the encoded cookie too large
	token.AccessToken = ""
	// mannually setting expiry for testing ... REMOVE
	token.Expiry = time.Now().Add(time.Second * time.Duration(30))
	return &CookieValue{
		UserID:          claims["oid"].(string),
		UserName:        claims["name"].(string),
		GroupMembership: groups.Groups,
		Token:           token,
	}, nil
}

func (a *Auth) getGroupMembership(token *oauth2.Token) (Groups, error) {
	var groups Groups
	var bearer = fmt.Sprintf("Bearer %s", token.AccessToken)
	req, err := http.NewRequest("GET", a.groupEndpoint, nil)
	req.Header.Add("Authorization", bearer)
	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return groups, errors.Wrap(err, "getting group membership info")
	}

	defer response.Body.Close()
	rawGroups, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return groups, errors.Wrap(err, "failed reading group membership response")
	}

	if err = json.Unmarshal(rawGroups, &groups); err != nil {
		return groups, errors.Wrap(err, "failed unmarshalling group membership response")
	}

	return groups, nil
}

func (a *Auth) readCookie(r *http.Request) (*CookieValue, error) {
	cookie, err := r.Cookie(a.cookieName)
	if err != nil {
		return nil, errors.Wrap(err, "cookie not found")
	}

	var value CookieValue
	err = a.secure.Decode(a.cookieName, cookie.Value, &value)
	if err != nil {
		return nil, errors.Wrap(err, "decoding cookie")
	}

	return &value, nil
}

func (a *Auth) setCookie(w http.ResponseWriter, cookie *CookieValue) error {
	encoded, err := a.secure.Encode(a.cookieName, cookie)
	if err != nil {
		return errors.Wrap(err, "encoding CookieValue")

	}
	newCookie := &http.Cookie{
		Name:     a.cookieName,
		Value:    encoded,
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		Expires:  cookie.Token.Expiry,
	}
	http.SetCookie(w, newCookie)
	return nil
}

func (a *Auth) refreshToken(w http.ResponseWriter, cookie *CookieValue) error {
	fmt.Println("REFRESHING TOKEN")
	if cookie.Token.RefreshToken == "" {
		return errors.New("no refresh token found, check auth scopes to see if refresh tokens are being provided by your IdP.")
	}
	tokenSource := a.oAuthConfig.TokenSource(context.Background(), cookie.Token)
	newToken, err := tokenSource.Token()
	if err != nil {
		return errors.Wrap(err, "refreshing token")
	}

	fmt.Printf("Refreshed AT: %v\n\n", newToken.AccessToken)

	if newToken.Expiry != cookie.Token.Expiry {
		cv, err := a.newCookieValue(newToken)
		if err != nil {
			errors.New("setting cookie")
		}

		a.setCookie(w, cv)
		fmt.Println("refreshed access token")
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
