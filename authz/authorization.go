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

	"github.com/molecula/featurebase/v2/authn"

	"gopkg.in/yaml.v2"
)

type GroupPermissions struct {
	Permissions map[string]map[string]string `yaml:"user-groups"`
	Admin       string                       `yaml:"admin"`
}

type Permission int64

const (
	None Permission = iota
	Read
	Write
	Admin
)

func (p *GroupPermissions) ReadPermissionsFile(permsFile io.Reader) (err error) {
	permsData, err := ioutil.ReadAll(permsFile)

	if err != nil {
		return fmt.Errorf("reading permissions failed with error: %s", err)
	}

	err = yaml.UnmarshalStrict(permsData, &p)
	if err != nil {
		return fmt.Errorf("unmarshalling permissions failed with error: %s", err)
	}

	return
}

func (p *GroupPermissions) GetPermissions(groups []authn.Group, index string) (permission string, errors error) {

	if admin := p.IsAdmin(groups); admin {
		return "admin", nil
	}

	allPermissions := map[string]bool{
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
				return "", fmt.Errorf("user %s does not have permission to index %s", group.UserID, index)
			}
		} else {
			groupsDenied = append(groupsDenied, group.GroupID)
		}
	}

	if len(groupsDenied) == len(groups) {
		return "", fmt.Errorf("group(s) %s does not have permission to FeatureBase", groupsDenied)
	}

	if allPermissions["write"] {
		return "write", nil
	} else if allPermissions["read"] {
		return "read", nil
	} else {
		return "", fmt.Errorf("no permissions found")
	}
}

func (p *GroupPermissions) IsAdmin(groups []authn.Group) bool {
	for _, group := range groups {
		if p.Admin == group.GroupID {
			return true
		}
	}
	return false
}

func (p *GroupPermissions) GetAuthorizedIndexList(groups []authn.Group, desiredPermission string) (indexList []string) {
	// if user is admin, find all indexes in permissions file and return them
	if admin := p.IsAdmin(groups); admin {
		for groupId := range p.Permissions {
			for index := range p.Permissions[groupId] {
				indexList = append(indexList, index)
			}
		}
		return indexList
	}

	for _, group := range groups {
		if _, ok := p.Permissions[group.GroupID]; ok {
			for index, permission := range p.Permissions[group.GroupID] {
				if permission == desiredPermission {
					indexList = append(indexList, index)
				} else if permission == "write" && desiredPermission == "read" {
					indexList = append(indexList, index)
				}
			}
		}
	}
	return indexList
}

func IsComparable(from, to string) bool {
	switch from {
	case "admin":
		return true
	case "write":
		if to == "write" || to == "read" {
			return true
		}
	case "read":
		if to == "read" {
			return true
		}
	}
	return false
}

func (p Permission) String() string {
	switch p {
	case Read:
		return "read"
	case Write:
		return "write"
	case Admin:
		return "admin"
	case None:
		return "none"
	}
	return "unknown"
}
