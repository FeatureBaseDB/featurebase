package models

import (
	"encoding/json"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/gofrs/uuid"
)

// NodeRoles holds information about what types of jobs (roles) each node can perform.
type NodeRole struct {
	ID        uuid.UUID    `json:"id" db:"id"`
	NodeID    uuid.UUID    `json:"node_id" db:"node_id"`
	Role      dax.RoleType `json:"role" db:"role"`
	CreatedAt time.Time    `json:"created_at" db:"created_at"`
	UpdatedAt time.Time    `json:"updated_at" db:"updated_at"`
}

// String is not required by pop and may be deleted
func (t *NodeRole) String() string {
	jt, _ := json.MarshalIndent(t, " ", " ") //nolint:errchkjson
	return string(jt)
}

type NodeRoles []NodeRole

func (t NodeRoles) String() string {
	jt, _ := json.MarshalIndent(t, " ", " ") //nolint:errchkjson
	return string(jt)
}
