package models

import (
	"encoding/json"
	"time"
)

// DirectiveVersion
type DirectiveVersion struct {
	ID        int       `json:"id" db:"id"`
	Version   int       `json:"version" db:"version"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// String is not required by pop and may be deleted
func (t *DirectiveVersion) String() string {
	jt, _ := json.MarshalIndent(t, " ", " ")
	return string(jt)
}
