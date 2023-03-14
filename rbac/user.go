package rbac

import (
	"fmt"
	"io/ioutil"
	"time"

	"gopkg.in/yaml.v2"
)

var (
	UserAPI *UserManager
)

func init() {
	UserAPI = NewUserManager()
}

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

type UserMessageType int

const (
	MAddUser = iota
	MGetUserByUsername
	MDelUser
)

type AddUserMessage struct {
	Username string
	Email    string
}

type GetUserByUsernameMessage struct {
	Username string
}

type DelUserMessage struct {
	username string
}

type UserManagerMessage struct {
	MessageType UserMessageType
	Message     interface{}
	AddUserMsg  *AddUserMessage
	GetUserMsg  *GetUserByUsernameMessage
	DelUserMsg  *DelUserMessage
}

type UserManager struct {
	Users []FBUser
	tx    chan UserManagerMessage
}

func NewUserManager() *UserManager {
	return &UserManager{
		Users: make([]FBUser, 0),
		tx:    make(chan UserManagerMessage),
	}
}

func (m *UserManager) Send(msg UserManagerMessage) {
	fmt.Printf("In UserManager Send")
	m.tx <- msg
}

func (m *UserManager) Start() {
	go func() {
		fmt.Printf("Starting UserManager listener...")
		for env := range m.tx {
			fmt.Printf("Received a message in UserManager: %v", env)
			switch env.MessageType {
			case MAddUser:
				amu := env.Message.(AddUserMessage)
				newUser := NewFBUser(amu.Username, "")
				fmt.Printf("Made new user: %s", newUser.Username)
			case MGetUserByUsername:
				env.Message = env.Message.(GetUserByUsernameMessage)
			case MDelUser:
				env.Message = env.Message.(DelUserMessage)
			}
		}
	}()
}
