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
	"strings"
	"testing"

	"github.com/molecula/featurebase/v2/authz"
)

func TestAuth_ReadPermissionsFile(t *testing.T) {
	var singleInput = `group_permissions:
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee55906b"
    index: "test"
    permission: "read"`

	var emptyInput = `group_permissions:
  - group:
    groupId: ""
    index: ""
    permission: ""`

	var multiInput = `group_permissions:
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee55906b"
    index: "test"
    permission: "read"
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee559900"
    index: "test"
    permission: "admin"`

	singleStruct := authz.GroupPermissions{
		Permissions: []authz.Permissions{
			{"dca35310-ecda-4f23-86cd-876aee55906b", "test", "read"},
		},
	}
	emptyStruct := authz.GroupPermissions{
		Permissions: []authz.Permissions{
			{"", "", ""},
		},
	}
	multiStruct := authz.GroupPermissions{
		Permissions: []authz.Permissions{
			{"dca35310-ecda-4f23-86cd-876aee55906b", "test", "read"},
			{"dca35310-ecda-4f23-86cd-876aee559900", "test", "admin"},
		},
	}

	tests := []struct {
		input  string
		output authz.GroupPermissions
	}{
		{singleInput, singleStruct},
		{emptyInput, emptyStruct},
		{multiInput, multiStruct},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			permFile := strings.NewReader(test.input)
			var p authz.GroupPermissions
			if err := p.ReadPermissionsFile(permFile); err != nil {
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
	var permissions1 = `group_permissions:
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee55906b"
    index: "test"
    permission: "read"`

	var permissions2 = `group_permissions:
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee559900"
    index: "test"
    permission: "read"
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee559900"
    index: "test"
    permission: "write"`

	var permissions3 = `group_permissions:
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee559900"
    index: "test"
    permission: "read"
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee559900"
    index: "test"
    permission: "admin"`

	var permissions4 = `group_permissions:
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee55906b"
    index: "test"
    permission: ""
  - group:
    groupId: "dca35310-ecda-4f23-86cd-876aee559900"
    index: "test"
    permission: "admin"`

	// initializes groups that are returned from identity provider
	groupsList1 := []authz.Group{}
	groupsList2 := []authz.Group{{"dca35310-ecda-4f23-86cd-876aee55906b", "name"}}
	groupsList3 := []authz.Group{
		{"dca35310-ecda-4f23-86cd-876aee55906b", "name"},
		{"dca35310-ecda-4f23-86cd-876aee559900", "name"},
	}

	tests := []struct {
		yamlData   string
		groups     []authz.Group
		index      []string
		userAccess string
		err        string
	}{
		{
			permissions1,
			groupsList1,
			[]string{"test"},
			"",
			"NOT allowed access to FeatureBase",
		},
		{
			permissions1,
			groupsList2,
			[]string{"test1"},
			"",
			"NOT allowed access to index",
		},
		{
			permissions1,
			groupsList2,
			[]string{"test"},
			"read",
			"",
		},
		{
			permissions2,
			groupsList3,
			[]string{"test"},
			"write",
			"",
		},
		{
			permissions3,
			groupsList3,
			[]string{"test"},
			"admin",
			"",
		},
		{
			permissions2,
			groupsList3,
			[]string{"test"},
			"write",
			"",
		},
		{
			permissions4,
			groupsList2,
			[]string{"test"},
			"",
			"no permissions found",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {

			permFile := strings.NewReader(test.yamlData)
			var p authz.GroupPermissions
			p.ReadPermissionsFile(permFile)

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
