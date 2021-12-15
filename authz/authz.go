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

package authz

import (
	"fmt"
	"io"
	"io/ioutil"

	"gopkg.in/yaml.v2"
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

	// Permissions file for groups
	PermissionsFile string `toml:"permissions"`
}

type GroupPermissions struct {
	Permissions []Permissions `yaml:"group_permissions"`
}

type Permissions struct {
	GroupId    string `yaml:"groupId"`
	Index      string `yaml:"index"`
	Permission string `yaml:"permission"`
}

type Group struct {
	ID   string `json:"id"`
	Name string `json:"displayName"`
}

func (p *GroupPermissions) ReadPermissionsFile(permsFile io.Reader) (err error) {
	permsData, err := ioutil.ReadAll(permsFile)
	if err != nil {
		return fmt.Errorf("reading permissions failed with error: %s", err)
	}

	err = yaml.Unmarshal(permsData, p)
	if err != nil {
		return fmt.Errorf("unmarshalling permissions failed with error: %s", err)
	}

	return nil
}

func (p *GroupPermissions) GetPermissions(groups []Group, index []string) (permission string, err error) {

	// get union of groups the user is part of obtained from identity provider and groups in permissions file
	var groupMatch []Permissions
	for _, group := range groups {
		for i := range p.Permissions {
			if group.ID == p.Permissions[i].GroupId {
				groupMatch = append(groupMatch, p.Permissions[i])
			}
		}
	}

	if len(groupMatch) == 0 {
		return "", fmt.Errorf("the user's groups %s are NOT allowed access to FeatureBase", groups)
	}

	// check that user's groups have access to the index user want to access
	var indexMatch []Permissions
	indexCheck := map[string]bool{}
	for _, g := range groupMatch {
		for _, idx := range index {
			if idx == g.Index {
				indexMatch = append(indexMatch, g)
				indexCheck[idx] = true
			}
		}
	}

	// check which index user does NOT have access to, and return in error mesg
	if len(indexCheck) != len(index) {
		var indexNotFound []string
		for _, idx := range index {
			if !indexCheck[idx] {
				indexNotFound = append(indexNotFound, idx)
			}
		}
		return "", fmt.Errorf("user is NOT allowed access to index: %s", indexNotFound)
	}

	// check permissions for index user has access to
	allPermissions := map[string]bool{
		"admin": false,
		"write": false,
		"read":  false,
	}

	for _, g := range indexMatch {
		allPermissions[g.Permission] = true
	}

	if allPermissions["admin"] {
		return "admin", nil
	} else if allPermissions["write"] {
		return "write", nil
	} else if allPermissions["read"] {
		return "read", nil
	} else {
		return "", fmt.Errorf("no permissions found")
	}
}
