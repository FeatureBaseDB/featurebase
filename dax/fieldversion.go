package dax

import "fmt"

// FieldVersion is used in a similar way to Shard and Partition in that they all
// contain a snapshot version. It would have been confusing to use the Field
// type which already exists, because versioning that would mean something else.
// This is really snapshot specific (as are Shard and Partition).
type FieldVersion struct {
	Name    FieldName `json:"name"`
	Version int       `json:"version"`
}

// String returns the FieldVersion (i.e. its Name and Version) as a string.
func (f FieldVersion) String() string {
	return fmt.Sprintf("%s.%d", f.Name, f.Version)
}

// NewFieldVersion returns a FieldVersion with the provided name and version.
func NewFieldVersion(name FieldName, version int) FieldVersion {
	return FieldVersion{
		Name:    name,
		Version: version,
	}
}

// FieldVersions is a sortable slice of FieldVersion.
type FieldVersions []FieldVersion

func (f FieldVersions) Len() int           { return len(f) }
func (f FieldVersions) Less(i, j int) bool { return f[i].Name < f[j].Name }
func (f FieldVersions) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
