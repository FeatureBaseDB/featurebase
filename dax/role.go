package dax

// RoleType represents a role type which a worker node can act as.
type RoleType string

const (
	RoleTypeCompute   RoleType = "compute"
	RoleTypeTranslate RoleType = "translate"
	RoleTypeQuery     RoleType = "query"
)

var (
	AllRoleTypes = []RoleType{RoleTypeCompute, RoleTypeTranslate, RoleTypeQuery}
)

// RoleTypes is a list of RoleType, used primarily to introduce helper methods
// on the list.
type RoleTypes []RoleType

// Contains returns true if the given RoleType is in RoleTypes.
func (rt RoleTypes) Contains(t RoleType) bool {
	for i := range rt {
		if rt[i] == t {
			return true
		}
	}
	return false
}

// Role is an interface for any role which a worker node can assume.
type Role interface {
	Type() RoleType
}

// Ensure type implements interface.
var _ Role = (*ComputeRole)(nil)
var _ Role = (*TranslateRole)(nil)

// ComputeRole is a role specific to compute nodes.
type ComputeRole struct {
	TableKey TableKey  `json:"table-key"`
	Shards   ShardNums `json:"shards"`
}

// Type returns the type for ComputeRole. This is mainly to implement the Role
// interface.
func (cr *ComputeRole) Type() RoleType {
	return RoleTypeCompute
}

// TranslateRole is a role specific to translate nodes.
type TranslateRole struct {
	TableKey   TableKey      `json:"table-key"`
	Partitions PartitionNums `json:"partitions"`
	Fields     []FieldName   `json:"fields"`
}

// Type returns the type for TranslateRole. This is mainly to implement the Role
// interface.
func (cr *TranslateRole) Type() RoleType {
	return RoleTypeTranslate
}
