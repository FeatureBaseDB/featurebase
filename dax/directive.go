package dax

import "context"

// Directive contains the instructions, sent from MDS, which a compute node is
// to follow. A Directive is typically JSON-encoded and POSTed to a compute
// node's `/directive` endpoint.
type Directive struct {
	Address Address `json:"address"`

	// Method describes how the compute node should handle the Directive. See
	// the different constants of type DirectiveMethod for how this value is
	// handled.
	Method DirectiveMethod `json:"method"`

	Tables []*QualifiedTable `json:"schema"`

	ComputeRoles   []ComputeRole   `json:"compute-roles"`
	TranslateRoles []TranslateRole `json:"translate-roles"`

	Version uint64 `json:"version"`
}

type DirectiveVersion interface {
	Increment(ctx context.Context, delta uint64) (uint64, error)
}

// DirectiveMethod is used to tell the compute node how it should handle the
// Directive.
type DirectiveMethod string

const (
	// DirectiveMethodDiff tells the compute node to diff the Directive with its
	// local, cached Directive and only apply the differences.
	DirectiveMethodDiff DirectiveMethod = "diff"

	// DirectiveMethodReset tells the compute node to delete all of its existing
	// data before applying the directive.
	DirectiveMethodReset DirectiveMethod = "reset"

	// DirectiveMethodSnapshot tells the compute node that the incoming
	// Directive should only contain data version updates related to a snapshot
	// request.
	DirectiveMethodSnapshot DirectiveMethod = "snapshot"
)

// Table returns the ID'd table from the Directive's Tables list. If it's not
// found, it returns nil and a non-nil error. A nil error guarantees that the
// returned table is non-nil.
func (d *Directive) Table(qtid QualifiedTableID) (*QualifiedTable, error) {
	for _, qtbl := range d.Tables {
		// We can't do qtbl.QualifiedID() == qtid because the value of qtid.Name
		// is empty and causes the equality check to fail. Hence the .Equals()
		// method.
		if qtbl.QualifiedID().Equals(qtid) {
			return qtbl, nil
		}
	}
	return nil, NewErrTableIDDoesNotExist(qtid)
}

// ComputeShards returns the list of shards, for the given table, for which this
// compute node is responsible. It assumes that the Directive does not contain
// more than one ComputeRole for the same table; in that case, we would need to
// return the union of Shards.
func (d *Directive) ComputeShards(tbl TableKey) ShardNums {
	if d == nil || d.ComputeRoles == nil {
		return nil
	}

	for _, cr := range d.ComputeRoles {
		if cr.TableKey == tbl {
			return cr.Shards
		}
	}

	return nil
}

// ComputeShardsMap returns a map of table to shards. It assumes that the
// Directive does not contain more than one ComputeRole for the same table; in
// that case, we would need to return the union of Shards.
func (d *Directive) ComputeShardsMap() map[TableKey]ShardNums {
	m := make(map[TableKey]ShardNums)
	if d == nil || d.ComputeRoles == nil {
		return m
	}

	for _, cr := range d.ComputeRoles {
		m[cr.TableKey] = cr.Shards
	}

	return m
}

// TranslatePartitions returns the list of partitions, for the given table, for
// which this translate node is responsible. It assumes that the Directive does
// not contain more than one TranslateRole for the same table; in that case, we
// would need to return the union of Shards.
func (d *Directive) TranslatePartitions(tbl TableKey) PartitionNums {
	if d == nil || d.TranslateRoles == nil {
		return PartitionNums{}
	}

	for _, tr := range d.TranslateRoles {
		if tr.TableKey == tbl {
			return tr.Partitions
		}
	}
	return PartitionNums{}
}

// TranslatePartitionsMap returns a map of table to partitions. It assumes that
// the Directive does not contain more than one TranslateRole for the same
// table; in that case, we would need to return the union of Partitions.
func (d *Directive) TranslatePartitionsMap() map[TableKey]PartitionNums {
	m := make(map[TableKey]PartitionNums)
	if d == nil || d.TranslateRoles == nil {
		return m
	}

	for _, tr := range d.TranslateRoles {
		// Since we added FieldVersions to the TranslateRole, it's possible for
		// a TranslateRole to have an empty Partitions list. In that case, we
		// want to exclude that from the map.
		if len(tr.Partitions) == 0 {
			continue
		}
		m[tr.TableKey] = tr.Partitions
	}

	return m
}

// TranslateFieldsMap returns a map of table to fields. It assumes that
// the Directive does not contain more than one TranslateRole for the same
// table; in that case, we would need to return the union of FieldValues.
func (d *Directive) TranslateFieldsMap() map[TableKey][]FieldName {
	m := make(map[TableKey][]FieldName)
	if d == nil || d.TranslateRoles == nil {
		return m
	}

	for _, tr := range d.TranslateRoles {
		if len(tr.Fields) == 0 {
			continue
		}
		m[tr.TableKey] = tr.Fields
	}

	return m
}

// IsEmpty tells whether a directive is assigning actual responsibilty
// to a node or not. If the directive does not assign responsibility
// for any shard or partition then it is considered empty. This is
// used to determine whether we can ignore an error received from
// applying this directive (an empty directive is often sent to a node
// which is already down).
func (d *Directive) IsEmpty() bool {
	for _, role := range d.ComputeRoles {
		if len(role.Shards) > 0 {
			return false
		}
	}

	for _, role := range d.TranslateRoles {
		if len(role.Partitions) > 0 {
			return false
		}
	}

	return true
}

// Directives is a sortable slice of Directive.
type Directives []*Directive

func (d Directives) Len() int           { return len(d) }
func (d Directives) Less(i, j int) bool { return d[i].Address.String() < d[j].Address.String() }
func (d Directives) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
