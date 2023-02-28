package rbac

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"

	"github.com/open-policy-agent/opa/sdk"
	sdktest "github.com/open-policy-agent/opa/sdk/test"
)

type ABACPolicyEnforcer struct {
	S      *sdk.OPA
	CTX    context.Context
	Server *sdktest.Server
}

func NewABACPolicyEnforcer() (*ABACPolicyEnforcer, error) {

	ctx := context.Background()

	policy, err := ioutil.ReadFile("example_policy.rego")
	if err != nil {
		return nil, err
	}
	policyS := string(policy)
	server, err := sdktest.NewServer(sdktest.MockBundle("/bundles/bundle.tar.gz", map[string]string{
		"example.rego": policyS,
	}))
	if err != nil {
		return nil, err
	}
	config := []byte(fmt.Sprintf(`{
		"services": {
			"test": {
				"url": %q
			}
		},
		"bundles": {
			"test": {
				"resource": "/bundles/bundle.tar.gz"
			}
		},
		"decision_logs": {
			"console": true
		}
	}`, server.URL()))
	opa, err := sdk.New(ctx, sdk.Options{
		Config: bytes.NewReader(config),
	})
	if err != nil {
		return nil, err
	}

	return &ABACPolicyEnforcer{
		S:      opa,
		CTX:    ctx,
		Server: server,
	}, nil

}

func readPolicyFile(p string) ([]byte, error) {
	policyData, err := ioutil.ReadFile(p)

	if err != nil {
		return nil, err
	}

	return policyData, nil

}
