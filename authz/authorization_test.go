// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package authz_test

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v2/authz"
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
		Permissions: map[string]map[string]string{
			"dca35310-ecda-4f23-86cd-876aee55906b": {"test": "read"},
		},
		Admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe",
	}

	multiPermission := authz.GroupPermissions{
		Permissions: map[string]map[string]string{
			"dca35310-ecda-4f23-86cd-876aee55906b": {"test": "read", "test2": "write"},
			"dca35310-ecda-4f23-86cd-876aee559900": {"test": "write"}},
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
	userId := "user-id"
	groupsList1 := []authz.Group{}
	groupsList2 := []authz.Group{{userId, "fake-group", groupName}}
	groupsList3 := []authz.Group{
		{userId, "dca35310-ecda-4f23-86cd-876aee55906b", groupName},
		{userId, "dca35310-ecda-4f23-86cd-876aee559900", groupName},
	}
	groupsList4 := []authz.Group{{userId, "ac97c9e2-346b-42a2-b6da-18bcb61a32fe", groupName}}

	tests := []struct {
		yamlData   string
		groups     []authz.Group
		index      string
		userAccess string
		err        string
	}{
		{
			permissions1,
			groupsList1,
			"test",
			"",
			"user is not part of any groups in identity provider",
		},
		{
			permissions1,
			groupsList3,
			"test1",
			"",
			"does not have permission to index",
		},
		{
			permissions2,
			groupsList2,
			"test",
			"",
			"does not have permission to FeatureBase",
		},
		{
			permissions1,
			groupsList3,
			"test",
			"read",
			"",
		},
		{
			permissions2,
			groupsList3,
			"test",
			"write",
			"",
		},
		{
			permissions3,
			groupsList4,
			"test",
			"admin",
			"",
		},
		{
			permissions4,
			groupsList3,
			"test",
			"",
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

			p1, err := p.GetPermissions(test.groups, test.index)

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

	group1 := []authz.Group{
		{"admin-user-id", "ac97c9e2-346b-42a2-b6da-18bcb61a32fe", "admin-group"},
	}

	group2 := []authz.Group{
		{"user-id", "dca35310-ecda-4f23-86cd-876aee55906b", "group-name"},
	}

	groupPermissions := authz.GroupPermissions{
		Permissions: map[string]map[string]string{
			"dca35310-ecda-4f23-86cd-876aee55906b": {"test": "write"},
		},
		Admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe",
	}

	tests := []struct {
		groups           []authz.Group
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

	group1 := []authz.Group{
		{"user-id", "dca35310-ecda-4f23-86cd-876aee55906b", "group-name"},
	}

	group2 := []authz.Group{
		{"admin-user-id", "ac97c9e2-346b-42a2-b6da-18bcb61a32fe", "admin-group"},
	}

	group3 := []authz.Group{
		{"user-id", "dca35310-ecda-4f23-86cd-876aee559900", "group-name"},
	}

	p := authz.GroupPermissions{
		Permissions: map[string]map[string]string{
			"dca35310-ecda-4f23-86cd-876aee55906b": {
				"test1": "read",
				"test2": "write",
			},
			"dca35310-ecda-4f23-86cd-876aee559900": {
				"test3": "read",
			},
		},
		Admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe",
	}

	tests := []struct {
		groups     []authz.Group
		permission string
		output     []string
	}{
		{
			group1,
			"read",
			[]string{"test1", "test2"},
		},
		{
			group1,
			"write",
			[]string{"test2"},
		},
		{
			group3,
			"write",
			nil,
		},
		{
			group2,
			"read",
			[]string{"test1", "test2", "test3"},
		},
		{
			group2,
			"write",
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
