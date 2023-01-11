// Copyright 2023 Molecula Corp (DBA FeatureBase). All rights reserved.
package keys

// Index represents the name of a Featurebase index.
type Index string

// Field represents the name of a Featurebase field. It
// is only meaningful within the context of a parent index.
type Field string

// View represents the name of a Featurebase view. It is
// only meaningful within the context of a parent field.
type View string

// Shard represents the numeric shard ID of a shard. A shard
// is specific to an index, but *not* specific to a field or
// view.
type Shard uint64

// Fragment represents the full identifier of a Featurebase
// fragment.
type Fragment struct {
	Index
	Field
	View
	Shard
}

// Shards represents an unsorted list of shards.
type Shards map[Shard]struct{}

// ViewContents represents the set of shards a given view contains.
type ViewContents Shards

// helper function to protect us against the day when we need to track
// more than this inside a ViewContents.
func (v ViewContents) Shards() Shards {
	return Shards(v)
}

// FieldContents represents the ViewContents for the views in a given field.
type FieldContents map[View]ViewContents

// IndexContents represents the FieldContents for the fields in a given index.
type IndexContents map[Field]FieldContents

// DBContents represents the IndexContents for the indexes in a DB.
type DBContents map[Index]IndexContents
