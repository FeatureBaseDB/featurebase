package fbcloud

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/pkg/errors"
)

const (
	authFlow           = "USER_PASSWORD_AUTH"
	cognitoURLTemplate = "https://cognito-idp.%s.amazonaws.com"
)

type cognitoParameters struct {
	Email    string `json:"USERNAME"`
	Password string `json:"PASSWORD"`
}

type cognitoAuthRequest struct {
	AuthParameters cognitoParameters `json:"AuthParameters"`
	AuthFlow       string            `json:"AuthFlow"`
	AppClientID    string            `json:"ClientId"`
}

type cognitoAuthResult struct {
	IDToken string `json:"IdToken"`
}

type cognitoAuthResponse struct {
	Result cognitoAuthResult `json:"AuthenticationResult"`
}

func authenticate(clientID, region, email, password string) (string, error) {
	authPayload := cognitoAuthRequest{
		AuthParameters: cognitoParameters{
			Email:    email,
			Password: password,
		},
		AuthFlow:    authFlow,
		AppClientID: clientID,
	}

	data, err := json.Marshal(authPayload)
	if err != nil {
		return "", errors.Wrap(err, "marshaling json")
	}

	url := fmt.Sprintf(cognitoURLTemplate, region)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(data))
	if err != nil {
		return "", errors.Wrap(err, "creating authentication request object")
	}
	req.Header.Add("Content-Type", "application/x-amz-json-1.1")
	req.Header.Add("X-Amz-Target", "AWSCognitoIdentityProviderService.InitiateAuth")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "making request")
	}
	defer resp.Body.Close()

	fullbod, err := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK || err != nil {
		return "", errors.Errorf("HTTP status code=%d from Cognito authentication response. reading body: %v, body: '%s'", resp.StatusCode, err, fullbod)
	}

	var auth cognitoAuthResponse
	err = json.Unmarshal(fullbod, &auth)
	if err != nil {
		return "", errors.Wrap(err, "decoding cognito auth response")
	}

	return auth.Result.IDToken, nil
}
