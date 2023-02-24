package rbac

import "google.golang.org/genproto/googleapis/type/datetime"

type ACLResource int32

const (
	Index ACLResource = iota
)

type ACLAction int32

const (
	Read ACLAction = iota
	Write
	Update
	Grant
)

type ACL struct {
	User      *FBUser
	Resource  ACLResource
	CreatedBy *FBUser
	Created   datetime.DateTime
}
