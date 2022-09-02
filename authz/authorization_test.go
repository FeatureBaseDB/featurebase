// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package authz_test

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v3/authn"
	"github.com/molecula/featurebase/v3/authz"
)

func TestAuth_ReadPermissionsFile(t *testing.T) {

	singleInput := `user-groups:
  "dca35310-ecda-4f23-86cd-876aee55906b":
    "test": "read"
admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe"`

	multiInput := `user-groups:
  "dca35310-ecda-4f23-86cd-876aee55906b":
    "test": "read"
    "test2": "write"
  "dca35310-ecda-4f23-86cd-876aee559900":
    "test": "write"
admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe"`

	singlePermission := authz.GroupPermissions{
		Permissions: map[string]map[string]authz.Permission{
			"dca35310-ecda-4f23-86cd-876aee55906b": {"test": authz.Read},
		},
		Admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe",
	}

	multiPermission := authz.GroupPermissions{
		Permissions: map[string]map[string]authz.Permission{
			"dca35310-ecda-4f23-86cd-876aee55906b": {"test": authz.Read, "test2": authz.Write},
			"dca35310-ecda-4f23-86cd-876aee559900": {"test": authz.Write}},
		Admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe",
	}

	tests := []struct {
		input  string
		output authz.GroupPermissions
	}{
		{singleInput, singlePermission},
		{multiInput, multiPermission},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			permFile := strings.NewReader(test.input)

			var p authz.GroupPermissions
			err := p.ReadPermissionsFile(permFile)
			if err != nil {
				t.Fatalf("readPermissionsFile error: %s", err)
			}

			if !reflect.DeepEqual(p, test.output) {
				t.Fatalf("expected output %s, but got %s", test.output, p)
			}
		},
		)
	}
}

func TestAuth_GetPermissions(t *testing.T) {

	// initializes different example of permissions file in yaml
	permissions1 := `"user-groups":
  "dca35310-ecda-4f23-86cd-876aee55906b":
    "test": "read"
admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe"`

	permissions2 := `"user-groups":
  "dca35310-ecda-4f23-86cd-876aee559900":
    "test": "write"
admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe"`

	permissions3 := `"user-groups":
  "dca35310-ecda-4f23-86cd-876aee55906b":
    "test": "write"
    "test2": "read"
  "dca35310-ecda-4f23-86cd-876aee559900":
    "test": "read"
admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe"`

	permissions4 := `"user-groups":
  "dca35310-ecda-4f23-86cd-876aee559900":
    "test": ""
admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe"`

	// initializes groups that are returned from identity provider
	groupName := "name"
	groupsList1 := []authn.Group{}
	groupsList2 := []authn.Group{{
		GroupID:   "fake-group",
		GroupName: groupName}}
	groupsList3 := []authn.Group{
		{GroupID: "dca35310-ecda-4f23-86cd-876aee55906b", GroupName: groupName},
		{GroupID: "dca35310-ecda-4f23-86cd-876aee559900", GroupName: groupName},
	}
	groupsList4 := []authn.Group{{GroupID: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe", GroupName: groupName}}

	tests := []struct {
		yamlData   string
		groups     []authn.Group
		index      string
		userAccess authz.Permission
		err        string
	}{
		{
			permissions1,
			groupsList1,
			"test",
			authz.None,
			"user is not part of any groups in identity provider",
		},
		{
			permissions1,
			groupsList3,
			"test1",
			authz.None,
			"does not have permission to index",
		},
		{
			permissions2,
			groupsList2,
			"test",
			authz.None,
			"does not have permission to FeatureBase",
		},
		{
			permissions1,
			groupsList3,
			"test",
			authz.Read,
			"",
		},
		{
			permissions2,
			groupsList3,
			"test",
			authz.Write,
			"",
		},
		{
			permissions3,
			groupsList4,
			"test",
			authz.Admin,
			"",
		},
		{
			permissions4,
			groupsList3,
			"test",
			authz.None,
			"no permissions found",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {

			permFile := strings.NewReader(test.yamlData)

			var p authz.GroupPermissions
			if err := p.ReadPermissionsFile(permFile); err != nil {
				t.Errorf("Error: %s", err)
			}

			p1, err := p.GetPermissions(&authn.UserInfo{Groups: test.groups}, test.index)

			if p1 != test.userAccess {
				t.Errorf("expected permission to be %s, but got %s", test.userAccess, p1)
			}

			if err != nil {
				if !strings.Contains(err.Error(), test.err) {
					t.Errorf("expected error to contain %s, but got %s", test.err, err.Error())
				}
			}

		})
	}
}

func TestAuth_IsAdmin(t *testing.T) {

	group1 := []authn.Group{
		{GroupID: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe", GroupName: "admin-group"},
	}

	group2 := []authn.Group{
		{GroupID: "dca35310-ecda-4f23-86cd-876aee55906b", GroupName: "group-name"},
	}

	groupPermissions := authz.GroupPermissions{
		Permissions: map[string]map[string]authz.Permission{
			"dca35310-ecda-4f23-86cd-876aee55906b": {"test": authz.Write},
		},
		Admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe",
	}

	tests := []struct {
		groups           []authn.Group
		groupPermissions authz.GroupPermissions
		output           bool
	}{
		{
			group1, groupPermissions, true,
		},
		{
			group2, groupPermissions, false,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			p := test.groupPermissions
			resp := p.IsAdmin(test.groups)
			if resp != test.output {
				t.Errorf("expected %t, but got %t", test.output, resp)
			}
		})
	}
}

func TestAuth_GetAuthorizedIndexList(t *testing.T) {

	group1 := []authn.Group{
		{GroupID: "dca35310-ecda-4f23-86cd-876aee55906b", GroupName: "group-name"},
	}

	group2 := []authn.Group{
		{GroupID: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe", GroupName: "admin-group"},
	}

	group3 := []authn.Group{
		{GroupID: "dca35310-ecda-4f23-86cd-876aee559900", GroupName: "group-name"},
	}

	p := authz.GroupPermissions{
		Permissions: map[string]map[string]authz.Permission{
			"dca35310-ecda-4f23-86cd-876aee55906b": {
				"test1": authz.Read,
				"test2": authz.Write,
			},
			"dca35310-ecda-4f23-86cd-876aee559900": {
				"test3": authz.Read,
			},
		},
		Admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe",
	}

	tests := []struct {
		groups     []authn.Group
		permission authz.Permission
		output     []string
	}{
		{
			group1,
			authz.Read,
			[]string{"test1", "test2"},
		},
		{
			group1,
			authz.Write,
			[]string{"test2"},
		},
		{
			group3,
			authz.Write,
			nil,
		},
		{
			group2,
			authz.Read,
			[]string{"test1", "test2", "test3"},
		},
		{
			group2,
			authz.Write,
			[]string{"test1", "test2", "test3"},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {

			indexList := p.GetAuthorizedIndexList(test.groups, test.permission)
			sort.Strings(indexList)

			if !reflect.DeepEqual(indexList, test.output) {
				t.Errorf("expected %s, but got %s", test.output, indexList)
			}
		})
	}

}
