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

package auth

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"

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

func ReadPermissionsFile(filePath string) (yamlData []byte) {
	filePathAbs, _ := filepath.Abs(filePath)
	yamlData, err := ioutil.ReadFile(filePathAbs)
	if err != nil {
		panic(err)
	}
	return yamlData
}

func (p *GroupPermissions) CreatePermissionsStruct(data []byte) {
	err := yaml.Unmarshal([]byte(data), &p)
	if err != nil {
		log.Fatalf("Error %s", err)
	}
}

func GetPermissions(Auth *Auth, groups []map[string]string, index []string) (permission string, err error) {
	// read yaml permissions file
	yamlData := ReadPermissionsFile(Auth.PermissionsFile)

	// get group permissions
	var p GroupPermissions
	p.CreatePermissionsStruct(yamlData)

	// check permissions for all groups and index, and return most permissive
	return p.ResolvePermissions(groups, index)
}

func (p *GroupPermissions) ResolvePermissions(groups []map[string]string, index []string) (permission string, err error) {

	// get union of groups the user is part of obtained from identity provider and groups in permissions file
	var groupMatch []Permissions
	for _, group := range groups {
		for i := range p.Permissions {
			if group["id"] == p.Permissions[i].GroupId {
				groupMatch = append(groupMatch, p.Permissions[i])
			}
		}
	}

	if len(groupMatch) == 0 {
		return "", fmt.Errorf("User is NOT allowed access to FeatureBase")
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

	// check that user has access to every index
	indexCount := 0
	for _, value := range indexCheck {
		if value {
			indexCount += 1
		}
	}

	if indexCount != len(index) {
		return "", fmt.Errorf("User is not allowed access to index: %s", index)
	}

	// check permissions for index user has access to
	allPermissions := map[string]bool{
		"admin": false,
		"write": false,
		"read":  false,
	}

	for _, g := range indexMatch {
		if !allPermissions[g.Permission] {
			allPermissions[g.Permission] = true
		}
	}

	if allPermissions["admin"] {
		return "admin", error(nil)
	} else if allPermissions["write"] {
		return "write", error(nil)
	} else if allPermissions["read"] {
		return "read", error(nil)
	} else {
		return "", fmt.Errorf("No permissions found")
	}
}
