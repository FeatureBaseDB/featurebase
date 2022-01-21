// Copyright 2021 Molecula Corp. All rights reserved.

// Package authn handles authentication
package authn

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
)

func init() {
	gob.Register([]Group{})
}

// Auth holds state, configuration, and utilities needed for authentication.
type Auth struct {
	logger         logger.Logger
	cookieName     string
	secretKey      []byte
	groupEndpoint  string
	logoutEndpoint string
	fbURL          string // fbURL is the domain FB is hosted on, used for post logout redirection
	oAuthConfig    *oauth2.Config
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
	}

	if auth.secretKey, err = decodeHex(secretKey); err != nil {
		return nil, errors.Wrap(err, "decoding secret key")
	}

	return auth, nil
}

func (a Auth) SecretKey() []byte {
	return a.secretKey
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

// ToGob64 encodes a []Group to a string, returning string and nil on success (todd's idea)
// it has to be a string bc we're using it in a jwt.MapClaims which needs string-y things
func ToGob64(m []Group) (string, error) {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(m); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b.Bytes()), nil
}

// FromGob64 converts a previously encoded []Group from a string to a []Group
// it has to be a string bc we're using it in a jwt.MapClaims which needs string-y things
func FromGob64(gobbed string) ([]Group, error) {
	m := []Group{}
	by, err := base64.StdEncoding.DecodeString(gobbed)
	if err != nil {
		return nil, err
	}
	b := bytes.Buffer{}
	b.Write(by)
	d := gob.NewDecoder(&b)
	err = d.Decode(&m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// Groups holds a slice of Group for marshalling from Json
type Groups struct {
	Groups []Group `json:"value"`
}

// Authenticate takes in a bearer token `bearer` and returns UserInfo from that token
func (a *Auth) Authenticate(bearer string) (*UserInfo, error) {
	// parse the bearer token into a jwt.Token
	token, err := jwt.Parse(bearer, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return a.secretKey, nil
	})
	if token == nil || token.Claims == nil || err != nil || !token.Valid {
		return nil, errors.Wrap(err, fmt.Sprintf("%#v parsing jwt claims from access tokens", token))
	}
	userInfo := UserInfo{}
	// check that token does not expire now
	switch claimType := token.Claims.(type) {
	case jwt.MapClaims:
		if exp, ok := claimType["exp"]; ok {
			var e int64
			switch expType := exp.(type) {
			case float64:
				e = int64(expType)
			case json.Number:
				e, _ = expType.Int64()
			}
			if e <= time.Now().Unix() {
				return nil, fmt.Errorf("token expired")
			}
		}

		userInfo.UserID = claimType["oid"].(string)
		userInfo.UserName = claimType["name"].(string)
		userInfo.Token = bearer

		g := claimType["molecula-idp-groups"].(string)
		groups, err := FromGob64(g)
		if err != nil {
			return nil, errors.Wrap(err, "decoding groups")
		}
		userInfo.Groups = groups

	default:
		return nil, fmt.Errorf("could not parse jwt claims of type %T, expected jwt.MapClaims", claimType)
	}

	return &userInfo, nil
}

// Login redirects a user to login to their configured oAuth authorize endpoint
func (a *Auth) Login(w http.ResponseWriter, r *http.Request) {
	authURL := a.oAuthConfig.AuthCodeURL(a.oAuthConfig.Endpoint.AuthURL)
	http.Redirect(w, r, authURL, http.StatusTemporaryRedirect)
}

// Logout clears out user cookie and redirects user to IdP's logout endpoint
func (a *Auth) Logout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     a.cookieName,
		Value:    "",
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Expires:  time.Unix(0, 0),
	})
	redirect := fmt.Sprintf("%s?post_logout_redirect_uri=%s/", a.logoutEndpoint, a.fbURL)
	http.Redirect(w, r, redirect, http.StatusTemporaryRedirect)
}

// Redirect handles the oAuth /redirect endpoint. It gets user information from
// the identity provider and sets a secure cookie holding the user information
// signed by featurebase.
func (a *Auth) Redirect(w http.ResponseWriter, r *http.Request) {
	code := r.FormValue("code")
	token, err := a.getToken(r, code)
	if err != nil {
		a.logger.Warnf("getting token from IdP: %+v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	// with vitamin A!
	enrichedTkn, err := a.addGroupMembership(token.AccessToken)
	if err != nil {
		a.logger.Warnf("enriching token with group membership: %+v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	a.setCookie(w, enrichedTkn, token.Expiry)
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

// getToken exhanges authorization code for an oAuth2 token
func (a *Auth) getToken(r *http.Request, code string) (*oauth2.Token, error) {
	token, err := a.oAuthConfig.Exchange(r.Context(), code)
	if err != nil {
		return nil, errors.Wrap(err, "exchanging auth code for token")
	}
	return token, nil
}

// addGroupMembership is only called in `a.Redirect`. It adds groups to a jwt's
// claims, and signs it using `a.secretKey`.
func (a *Auth) addGroupMembership(token string) (string, error) {
	g, err := a.getGroups(token)
	if err != nil {
		return "", err
	}

	// parse token into jwt
	unenriched, _, err := new(jwt.Parser).ParseUnverified(token, jwt.MapClaims{})
	if unenriched == nil || unenriched.Claims == nil || err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("%v parsing jwt claims from access tokens", token))
	}

	enriched := jwt.New(jwt.SigningMethodHS256)
	enriched.Claims = unenriched.Claims
	var tokenStr string
	// parse groups into string format
	switch claims := enriched.Claims.(type) {
	case jwt.MapClaims:
		groupString, err := ToGob64(g)
		if err != nil {
			return "", errors.Wrap(err, "failed to serialize groups")
		}

		// stick it into jwt claims
		claims["molecula-idp-groups"] = groupString

		// get stringified and signed jwt
		tokenStr, err = enriched.SignedString(a.secretKey)
		if err != nil {
			return "", errors.Wrap(err, "signing jwt")
		}

	default:
		return "", fmt.Errorf("could not parse jwt claims of type %T, expected jwt.MapClaims", claims)
	}

	return tokenStr, nil
}

// getGroups gets the group membership for a given token from configured IdP
func (a *Auth) getGroups(token string) ([]Group, error) {
	var groups Groups

	req, err := http.NewRequest("GET", a.groupEndpoint, nil)
	if err != nil {
		return groups.Groups, errors.Wrap(err, "creating new request to group endpoint")
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return groups.Groups, errors.Wrap(err, "getting group membership info")
	}

	defer response.Body.Close()
	rawGroups, err := io.ReadAll(response.Body)
	if err != nil {
		return groups.Groups, errors.Wrap(err, "failed reading group membership response")
	}

	if err = json.Unmarshal(rawGroups, &groups); err != nil {
		return groups.Groups, errors.Wrap(err, "failed unmarshalling group membership response")
	}

	return groups.Groups, nil
}

func (a *Auth) setCookie(w http.ResponseWriter, token string, expiry time.Time) error {
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
