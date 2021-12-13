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
package auth_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v2/auth"
)

func TestAuth_CreatePermissionsStruct(t *testing.T) {
	var singleInput = []byte(`group_permissions:
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee55906b"
    index: "test"
    permission: "read"`)

	var emptyInput = []byte(`group_permissions:
  - group:
    groupId: ""
    index: ""
    permission: ""`)

	var multiInput = []byte(`group_permissions:
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee55906b"
    index: "test"
    permission: "read"
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee559900"
    index: "test"
    permission: "admin"`)

	singleStruct := auth.GroupPermissions{
		Permissions: []auth.Permissions{
			{"dca35310-ecda-4f23-86cd-876aee55906b", "test", "read"},
		},
	}
	emptyStruct := auth.GroupPermissions{
		Permissions: []auth.Permissions{
			{"", "", ""},
		},
	}
	multiStruct := auth.GroupPermissions{
		Permissions: []auth.Permissions{
			{"dca35310-ecda-4f23-86cd-876aee55906b", "test", "read"},
			{"dca35310-ecda-4f23-86cd-876aee559900", "test", "admin"},
		},
	}

	tests := []struct {
		input  []byte
		output auth.GroupPermissions
	}{
		{singleInput, singleStruct},
		{emptyInput, emptyStruct},
		{multiInput, multiStruct},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			var p auth.GroupPermissions
			p.CreatePermissionsStruct(test.input)

			if !reflect.DeepEqual(p, test.output) {
				t.Fatalf("expected output %s, but got %s", test.output, p)
			}
		},
		)
	}
}

func createGroupMaps(groups []string) []map[string]string {

	var group1 []map[string]string
	for _, i := range groups {
		map1 := map[string]string{}
		map1["id"] = i
		group1 = append(group1, map1)
	}
	return group1
}

func TestAuth_ResolvePermissions(t *testing.T) {
	// initializes different example of permissions file in yaml
	var permissions1 = []byte(`group_permissions:
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee55906b"
    index: "test"
    permission: "read"`)

	var permissions2 = []byte(`group_permissions:
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee559900"
    index: "test"
    permission: "read"
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee559900"
    index: "test"
    permission: "write"`)

	var permissions3 = []byte(`group_permissions:
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee559900"
    index: "test"
    permission: "read"
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee559900"
    index: "test"
    permission: "admin"`)

	var permissions4 = []byte(`group_permissions:
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee55906b"
    index: "test"
    permission: ""
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee559900"
    index: "test"
    permission: "admin"`)

	// initializes groups that are returned from identity provider
	groupsList1 := []string{}
	groupsList2 := []string{"dca35310-ecda-4f23-86cd-876aee55906b"}
	groupsList3 := []string{"dca35310-ecda-4f23-86cd-876aee55906b", "dca35310-ecda-4f23-86cd-876aee559900"}

	tests := []struct {
		permissions []byte
		groups      []map[string]string
		index       []string
		userAccess  string
		err         string
	}{
		{
			permissions1,
			createGroupMaps(groupsList1),
			[]string{"test"},
			"",
			"user is NOT allowed access to FeatureBase",
		},
		{
			permissions1,
			createGroupMaps(groupsList2),
			[]string{"test1"},
			"",
			"user is not allowed access to index",
		},
		{
			permissions1,
			createGroupMaps(groupsList2),
			[]string{"test"},
			"read",
			"",
		},
		{
			permissions2,
			createGroupMaps(groupsList3),
			[]string{"test"},
			"write",
			"",
		},
		{
			permissions3,
			createGroupMaps(groupsList3),
			[]string{"test"},
			"admin",
			"",
		},
		{
			permissions2,
			createGroupMaps(groupsList3),
			[]string{"test"},
			"write",
			"",
		},
		{
			permissions4,
			createGroupMaps(groupsList2),
			[]string{"test"},
			"",
			"no permissions found",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {

			var p auth.GroupPermissions
			p.CreatePermissionsStruct(test.permissions)

			p1, err := p.ResolvePermissions(test.groups, test.index)

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
