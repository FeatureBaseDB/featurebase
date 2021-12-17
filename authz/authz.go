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
	Permissions map[string]map[string]string
}

type Group struct {
	UserID    string
	GroupID   string `json:"id"`
	GroupName string `json:"displayName"`
}

func (p *GroupPermissions) ReadPermissionsFile(permsFile io.Reader) (err error) {
	permsData, err := ioutil.ReadAll(permsFile)

	if err != nil {
		return fmt.Errorf("reading permissions failed with error: %s", err)
	}

	err = yaml.UnmarshalStrict(permsData, &p.Permissions)
	if err != nil {
		return fmt.Errorf("unmarshalling permissions failed with error: %s", err)
	}

	return nil
}

func (p *GroupPermissions) GetPermissions(groups []Group, index string) (permission string, errors error) {

	allPermissions := map[string]bool{
		"admin": false,
		"write": false,
		"read":  false,
	}

	if len(groups) == 0 {
		return "", fmt.Errorf("user is not part of any groups in identity provider")
	}

	var groupsDenied []string
	for _, group := range groups {
		if _, ok := p.Permissions[group.GroupID]; ok {
			if perm, ok := p.Permissions[group.GroupID][index]; ok {
				allPermissions[perm] = true
			} else {
				return "", fmt.Errorf("User %s is NOT allowed access to index %s", group.UserID, index)
			}
		} else {
			groupsDenied = append(groupsDenied, group.GroupID)
		}
	}

	if len(groupsDenied) == len(groups) {
		return "", fmt.Errorf("group(s) %s are NOT allowed access to FeatureBase", groupsDenied)
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
