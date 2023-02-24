package rbac

import "google.golang.org/genproto/googleapis/type/datetime"

type FBUserStatus int32

const (
	Active FBUserStatus = 0
	Disabled
	Locked
)

type FBUser struct {
	Username     string
	Email        string
	Created      datetime.DateTime
	LastActivity datetime.DateTime
	Status       FBUserStatus
}
