// Copyright 2021 Molecula Corp. All rights reserved.
package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
)

type Auth struct {
	// Enable AuthZ/AuthN for featurebase server
	Enable bool `toml:"enable"`

	// Application/Client ID
	ClientId string `toml:"client-id"`

	// Client Secret
	ClientSecret string `toml:"client-secret"`

	// Authorize URL
	AuthorizeURL string `toml:"authorize-url"`

	// Token URL
	TokenURL string `toml:"token-url"`

	// Group Endpoint URL
	GroupEndpointURL string `toml:"group-endpoint-url"`

	// Scope URL
	ScopeURL string `toml:"scope-url"`
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

func Authenticate(w http.ResponseWriter, r *http.Request) []Group {
	cookie, err := readCookie(r)
	if err != nil {
		//add logging
		http.Redirect(w, r, "/login", http.StatusTemporaryRedirect)
		return nil
	}
	if cookie.Token.Expiry.Before(time.Now().Add(refreshWithin)) {
		err = cookie.refreshToken(w)
		if err != nil {
			http.Redirect(w, r, "/login", http.StatusTemporaryRedirect)
			return nil
		}
	}
	return cookie.GroupMembership

}

func Login(w http.ResponseWriter, r *http.Request) {
	log.Infof("/login")
	authUrl := OauthConfig.AuthCodeURL(OauthConfig.Endpoint.AuthURL)
	log.Infof("AUTHURL: %v\n\n", authUrl)
	http.Redirect(w, r, authUrl, http.StatusTemporaryRedirect)
}

// Gets user information from IdP and sets a secure cookie
func Redirect(w http.ResponseWriter, r *http.Request) {
	log.Infof("/redirect")
	code := r.FormValue("code")
	log.Infof("CODE %v\n\n", code)
	token, err := getToken(code)
	log.Infof("TOKEN %v\n\n", token)
	if err != nil {
		errors.Wrap(err, "getting token")
		http.Redirect(w, r, "/login", http.StatusTemporaryRedirect)
	}
	fmt.Printf("TOKEN %v\n\n", token)

	cv := newCookieValue(token)
	cv.setCookie(w)
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

func getToken(code string) (*oauth2.Token, error) {
	token, err := OauthConfig.Exchange(context.Background(), code)
	if err != nil {
		return nil, errors.Wrap(err, "exchanging auth code for token")
	}
	return token, nil
}

func newCookieValue(token *oauth2.Token) *CookieValue {
	accessParsed, err := jwt.Parse(token.AccessToken, nil)
	if token == nil {
		fmt.Println(errors.Wrap(err, "parsing jwt claims from access tokens"))
	}
	claims := accessParsed.Claims.(jwt.MapClaims)

	groups, err := getGroupMembership(token)
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

func getGroupMembership(token *oauth2.Token) (Groups, error) {
	var groups Groups
	var bearer = fmt.Sprintf("Bearer %s", token.AccessToken)
	req, err := http.NewRequest("GET", groupEndpoint, nil)
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

func readCookie(r *http.Request) (*CookieValue, error) {
	cookie, err := r.Cookie(cookieName)
	if err != nil {
		return nil, errors.Wrap(err, "cookie not found")
	}

	var value CookieValue
	err = secure.Decode(cookieName, cookie.Value, &value)
	if err != nil {
		return nil, errors.Wrap(err, "decoding cookie")
	}

	return &value, nil
}

func (cookie *CookieValue) setCookie(w http.ResponseWriter) error {
	encoded, err := secure.Encode(cookieName, cookie)
	if err != nil {
		return errors.Wrap(err, "encoding CookieValue")

	}
	newCookie := &http.Cookie{
		Name:     cookieName,
		Value:    encoded,
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		Expires:  cookie.Token.Expiry,
	}
	http.SetCookie(w, newCookie)
	return nil
}

func (cookie *CookieValue) refreshToken(w http.ResponseWriter) error {
	fmt.Println("REFRESHING TOKEN")
	tokenSource := OauthConfig.TokenSource(context.Background(), cookie.Token)
	newToken, err := tokenSource.Token()
	if err != nil {
		return errors.Wrap(err, "refreshing token")
	}

	fmt.Printf("Refreshed AT: %v\n\n", newToken.AccessToken)

	if newToken.Expiry != cookie.Token.Expiry {
		cv := newCookieValue(newToken)
		cv.setCookie(w)
		fmt.Println("refreshed access token")
	}

	return nil
}
