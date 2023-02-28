package rbac_test

import (
	"testing"

	"github.com/featurebasedb/featurebase/v3/rbac"
)

func TestCreateUserFromYAML(t *testing.T) {
	users, err := rbac.LoadUsersFromYAML("users.yaml")
	if err != nil {
		t.Errorf("Error loading test users from YAML: %s", err)
	}

	if len(users) != 1 {
		t.Errorf("Expected 1 user to be loaded from YAML. Got %d", len(users))
	}

	if users[0].Username != "fletcher" {
		t.Errorf("Expected test user username to be fletcher. Got %s", users[0].Username)
	}

	if users[0].Email != "fletcher.haynes@featurebase.com" {
		t.Errorf("Expected test user email to be fletcher.haynes@featurebase.com. Got %s", users[0].Email)
	}

}
