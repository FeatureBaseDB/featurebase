// Copyright 2021 Molecula Corp. All rights reserved.
package auth

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
	logger        logger.Logger
	cookieName    string
	refreshWithin time.Duration
	hashKey       []byte
	blockKey      []byte
	secure        *securecookie.SecureCookie
	groupEndpoint string
	oAuthConfig   *oauth2.Config
}

func NewAuth(logger logger.Logger, url string, scopes []string, authUrl, tokenUrl, groupEndpoint, clientID, clientSecret, hashKey, blockKey string) (*Auth, error) {
	auth := &Auth{
		logger:        logger,
		cookieName:    "molecula-chip",
		refreshWithin: time.Second * time.Duration(15),
		groupEndpoint: groupEndpoint,
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
	data, err := decodeHex(hashKey)
	if err != nil {
		return nil, errors.Wrap(err, "decoding hash key")
	}
	auth.hashKey = data

	data, err = decodeHex(blockKey)
	if err != nil {
		return nil, errors.Wrap(err, "decoding block key")
	}
	auth.blockKey = data

	auth.secure = securecookie.New(auth.hashKey, auth.blockKey)

	auth.logger.Infof("AUTH: %+v", auth)

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
	ID   string `json:"id"`
	Name string `json:"displayName"`
}

func (a *Auth) Authenticate(w http.ResponseWriter, r *http.Request) []Group {
	cookie, err := a.readCookie(r)
	if err != nil {
		//add logging
		http.Redirect(w, r, "/login", http.StatusTemporaryRedirect)
		return nil
	}
	if cookie.Token.Expiry.Before(time.Now().Add(a.refreshWithin)) {
		err = a.refreshToken(w, cookie)
		if err != nil {
			http.Redirect(w, r, "/login", http.StatusTemporaryRedirect)
			return nil
		}
	}
	return cookie.GroupMembership

}

func (a *Auth) Login(w http.ResponseWriter, r *http.Request) {
	a.logger.Infof("/login")
	authUrl := a.oAuthConfig.AuthCodeURL(a.oAuthConfig.Endpoint.AuthURL)
	a.logger.Infof("AUTHURL: %v\n\n", authUrl)
	http.Redirect(w, r, authUrl, http.StatusTemporaryRedirect)
}

// Gets user information from dP and sets a secure cookie
func (a *Auth) Redirect(w http.ResponseWriter, r *http.Request) {
	code := r.FormValue("code")
	a.logger.Infof("CODE %v\n\n", code)
	token, err := a.getToken(code)
	a.logger.Infof("TOKEN %v\n\n", token)
	if err != nil {
		errors.Wrap(err, "getting token")
		http.Redirect(w, r, "/login", http.StatusTemporaryRedirect)
	}
	fmt.Printf("TOKEN %v\n\n", token)

	cv := a.newCookieValue(token)
	a.setCookie(w, cv)
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

func (a *Auth) getToken(code string) (*oauth2.Token, error) {
	token, err := a.oAuthConfig.Exchange(context.Background(), code)
	if err != nil {
		return nil, errors.Wrap(err, "exchanging auth code for token")
	}
	return token, nil
}

func (a *Auth) newCookieValue(token *oauth2.Token) *CookieValue {
	accessParsed, err := jwt.Parse(token.AccessToken, nil)
	if token == nil {
		fmt.Println(errors.Wrap(err, "parsing jwt claims from access tokens"))
	}
	claims := accessParsed.Claims.(jwt.MapClaims)

	groups, err := a.getGroupMembership(token)
	if err != nil {
		fmt.Println(errors.Wrap(err, "getting group memebership"))
	}
	// not needed anymore, and makes the encoded cookie too large
	token.AccessToken = ""
	// mannually setting expiry for testing ... REMOVE
	token.Expiry = time.Now().Add(time.Second * time.Duration(30))
	return &CookieValue{
		UserID:          claims["oid"].(string),
		UserName:        claims["name"].(string),
		GroupMembership: groups.Groups,
		Token:           token,
	}
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
	tokenSource := a.oAuthConfig.TokenSource(context.Background(), cookie.Token)
	newToken, err := tokenSource.Token()
	if err != nil {
		return errors.Wrap(err, "refreshing token")
	}

	fmt.Printf("Refreshed AT: %v\n\n", newToken.AccessToken)

	if newToken.Expiry != cookie.Token.Expiry {
		cv := a.newCookieValue(newToken)
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
