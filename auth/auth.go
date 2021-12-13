// Copyright 2021 Molecula Corp. All rights reserved.
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
	err := yaml.Unmarshal([]byte(data), p)
	if err != nil {
		log.Fatalf("error %s", err)
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
		return "", fmt.Errorf("user is NOT allowed access to FeatureBase")
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
		return "", fmt.Errorf("user is not allowed access to index: %s", indexNotFound)
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
