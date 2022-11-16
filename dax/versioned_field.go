package dax

import "fmt"

// VersionedField is used in a similar way to VersionedShard and
// VersionedPartition in that they all contain a snapshot version.
type VersionedField struct {
	Name    FieldName `json:"name"`
	Version int       `json:"version"`
}

// String returns the VersionedField (i.e. its Name and Version) as a string.
func (f VersionedField) String() string {
	return fmt.Sprintf("%s.%d", f.Name, f.Version)
}

// NewVersionedField returns a VersionedField with the provided name and version.
func NewVersionedField(name FieldName, version int) VersionedField {
	return VersionedField{
		Name:    name,
		Version: version,
	}
}

// VersionedFields is a sortable slice of VersionedField.
type VersionedFields []VersionedField

func (f VersionedFields) Len() int           { return len(f) }
func (f VersionedFields) Less(i, j int) bool { return f[i].Name < f[j].Name }
func (f VersionedFields) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
