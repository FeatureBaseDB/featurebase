package rbac

import (
	"fmt"
	"io/ioutil"
	"time"

	"gopkg.in/yaml.v2"
)

type FBUserStatus int32

const (
	Active FBUserStatus = 0
	Disabled
	Locked
)

type FBUser struct {
	Username     string `yaml:"username"`
	Email        string `yaml:"email"`
	Created      time.Time
	LastActivity time.Time
	Status       FBUserStatus
}

func NewFBUser(username, email string) FBUser {
	newUser := FBUser{
		Username:     username,
		Email:        email,
		Created:      time.Now(),
		LastActivity: time.Now(),
		Status:       Active,
	}
	return newUser
}
func LoadUsersFromYAML(p string) ([]FBUser, error) {
	usersData := make(map[string][]map[string]string)
	users := make([]FBUser, 0)
	data, err := ioutil.ReadFile(p)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(data, usersData)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Users are: %v", usersData)
	for _, user := range usersData["users"] {
		newUser := FBUser{
			Username: user["name"],
			Email:    user["email"],
		}
		users = append(users, newUser)
	}
	return users, nil

}
