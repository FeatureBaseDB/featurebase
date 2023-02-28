package rbac_test

import (
	"testing"

	"github.com/featurebasedb/featurebase/v3/rbac"
	"github.com/open-policy-agent/opa/sdk"
)

func TestCreateABACPolicyEnforcer(t *testing.T) {
	enf, err := rbac.NewABACPolicyEnforcer()
	if err != nil {
		t.Errorf("Error creating ABACPolicyEnforcer: %s", err)
	}

	defer enf.Server.Stop()
	defer enf.S.Stop(enf.CTX)

	result, err := enf.S.Decision(enf.CTX, sdk.DecisionOptions{Path: "/featurebase/authz/allow", Input: map[string]interface{}{"open": "sesame"}})
	if err != nil {
		t.Errorf("Error testing OPA decision: %s", err)
	}
	if result.Result.(bool) != false {
		t.Errorf("Result was not false!")
	}

	result, err = enf.S.Decision(enf.CTX, sdk.DecisionOptions{Path: "/featurebase/authz/allow", Input: map[string]interface{}{"method": "SELECT", "path": "index_a", "user": "fletcher"}})

	if err != nil {
		t.Errorf("Error testing OPA decision: %s", err)
	}

	if result.Result.(bool) != true {
		t.Errorf("Result was not true!")
	}

}
