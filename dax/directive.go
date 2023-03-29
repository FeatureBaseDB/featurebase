package dax

import "sort"

// Directive contains the instructions, sent from the Controller, which a
// compute node is to follow. A Directive is typically JSON-encoded and POSTed
// to a compute node's `/directive` endpoint.
type Directive struct {
	Address Address `json:"address"`

	// Method describes how the compute node should handle the Directive. See
	// the different constants of type DirectiveMethod for how this value is
	// handled.
	Method DirectiveMethod `json:"method"`

	Tables []*QualifiedTable `json:"schema"`

	ComputeRoles   []ComputeRole   `json:"compute-roles"`
	TranslateRoles []TranslateRole `json:"translate-roles"`

	// The following members are used by DirectiveMethodDiff. They inlude only
	// those roles which have changed, as opposed to the entire role set for the
	// worker.
	ComputeRolesAdded     []ComputeRole   `json:"compute-roles-added"`
	ComputeRolesRemoved   []ComputeRole   `json:"compute-roles-removed"`
	TranslateRolesAdded   []TranslateRole `json:"translate-roles-added"`
	TranslateRolesRemoved []TranslateRole `json:"translate-roles-removed"`

	Version uint64 `json:"version"`
}

type DirectiveVersion interface {
	GetCurrent(tx Transaction, addr Address) (uint64, error)
	SetNext(tx Transaction, addr Address, current, next uint64) error
}

// DirectiveMethod is used to tell the compute node how it should handle the
// Directive.
type DirectiveMethod string

const (
	// DirectiveMethodFull tells the compute node consider the Directive as the
	// full, complete state to which it should adhere. It should diff the
	// Directive with its local, cached Directive and only apply the
	// differences.
	DirectiveMethodFull DirectiveMethod = "full"

	// DirectiveMethodFull includes only diffs. The compute node should keep
	// everything about its existing state the same, and just apply the diffs in
	// the Directive.
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

// computeShardsMapOfMaps returns a map of TableKey to a map of ShardNum in
// order to support adding and removing shards as distinct values. This map can
// then be converted back to a slice of ShardNum.
func (d *Directive) computeShardsMapOfMaps() map[TableKey]map[ShardNum]struct{} {
	m := make(map[TableKey]map[ShardNum]struct{})
	if d == nil || d.ComputeRoles == nil {
		return m
	}

	for _, cr := range d.ComputeRoles {
		m[cr.TableKey] = make(map[ShardNum]struct{})
		for _, shardNum := range cr.Shards {
			m[cr.TableKey][shardNum] = struct{}{}
		}
	}

	return m
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

// translatePartitionsMapOfMaps returns a map of TableKey to a map of
// PartitionNum in order to support adding and removing partitions as distinct
// values. This map can then be converted back to a slice of PartitionNum.
func (d *Directive) translatePartitionsMapOfMaps() map[TableKey]map[PartitionNum]struct{} {
	m := make(map[TableKey]map[PartitionNum]struct{})
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
		m[tr.TableKey] = make(map[PartitionNum]struct{})
		for _, partitionNum := range tr.Partitions {
			m[tr.TableKey][partitionNum] = struct{}{}
		}
	}

	return m
}

// translateFieldsMapOfMaps returns a map of TableKey to a map of FieldName in
// order to support adding and removing fields as distinct values. This map can
// then be converted back to a slice of FieldName.
func (d *Directive) translateFieldsMapOfMaps() map[TableKey]map[FieldName]struct{} {
	m := make(map[TableKey]map[FieldName]struct{})
	if d == nil || d.TranslateRoles == nil {
		return m
	}

	for _, tr := range d.TranslateRoles {
		if len(tr.Fields) == 0 {
			continue
		}
		m[tr.TableKey] = make(map[FieldName]struct{})
		for _, fname := range tr.Fields {
			m[tr.TableKey][fname] = struct{}{}
		}
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

// Copy returns a copy of Directive.
func (d *Directive) Copy() *Directive {
	ret := &Directive{
		Address: d.Address,
		Method:  d.Method,
		Version: d.Version,
	}
	ret.Tables = append(ret.Tables, d.Tables...)
	ret.ComputeRoles = append(ret.ComputeRoles, d.ComputeRoles...)
	ret.TranslateRoles = append(ret.TranslateRoles, d.TranslateRoles...)
	// We intenionally do not copy the `Added` and `Removed` members because
	// those are not necessary to keep in the cached Directive (which just needs
	// to include the full Directive); they are only required when sending the
	// diff Directive.
	return ret
}

// ApplyDiff applies the diffs specified in diff to d.
func (d *Directive) ApplyDiff(diff *Directive) *Directive {
	// Add any tables which are included in diff but not in d. We don't remove
	// tables based on a diff.
	for _, qtbl := range diff.Tables {
		if t, _ := d.Table(qtbl.QualifiedID()); t == nil {
			d.Tables = append(d.Tables, qtbl)
		}
	}

	// cmap is a map of map used to apply the directive diffs. We will convert
	// the final map to the ComputeRoles member in the returned Directive.
	cmap := d.computeShardsMapOfMaps()

	// Handle ComputeRolesAdded
	for _, crole := range diff.ComputeRolesAdded {
		if _, ok := cmap[crole.TableKey]; !ok {
			cmap[crole.TableKey] = make(map[ShardNum]struct{})
		}
		for _, shardNum := range crole.Shards {
			cmap[crole.TableKey][shardNum] = struct{}{}
		}
	}

	// Handle ComputeRolesRemoved
	for _, crole := range diff.ComputeRolesRemoved {
		if _, ok := cmap[crole.TableKey]; !ok {
			continue
		}
		for _, shardNum := range crole.Shards {
			delete(cmap[crole.TableKey], shardNum)
		}
	}

	// Convert cmap back to d.ComputeRoles.
	croles := make([]ComputeRole, 0, len(cmap))
	for tkey, smap := range cmap {
		shards := make([]ShardNum, 0, len(smap))
		for s := range smap {
			shards = append(shards, s)
		}
		sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
		croles = append(croles, ComputeRole{
			TableKey: tkey,
			Shards:   shards,
		})
	}
	// Sort croles by table.
	sort.Slice(croles, func(i, j int) bool { return croles[i].TableKey < croles[j].TableKey })
	d.ComputeRoles = croles

	// tmap is a map of map used to apply the directive diffs. We will convert
	// the final map to the TranslateRoles member in the returned Directive.
	tmap := d.translatePartitionsMapOfMaps()

	// tmapf is a map of map, specific to translate fields, used to apply the
	// directive diffs. We will convert the final map to the TranslateRoles
	// member in the returned Directive.
	tmapf := d.translateFieldsMapOfMaps()

	// Handle TransateRolesAdded
	for _, trole := range diff.TranslateRolesAdded {
		if len(trole.Fields) > 0 {
			// Fields.
			if _, ok := tmapf[trole.TableKey]; !ok {
				tmapf[trole.TableKey] = make(map[FieldName]struct{})
			}
			for _, fname := range trole.Fields {
				tmapf[trole.TableKey][fname] = struct{}{}
			}
		} else {
			// Partitions.
			if _, ok := tmap[trole.TableKey]; !ok {
				tmap[trole.TableKey] = make(map[PartitionNum]struct{})
			}
			for _, partitionNum := range trole.Partitions {
				tmap[trole.TableKey][partitionNum] = struct{}{}
			}
		}
	}

	// Handle TranslateRolesRemoved
	for _, trole := range diff.TranslateRolesRemoved {
		if len(trole.Fields) > 0 {
			// Fields.
			if _, ok := tmapf[trole.TableKey]; !ok {
				continue
			}
			for _, fname := range trole.Fields {
				delete(tmapf[trole.TableKey], fname)
			}
		} else {
			// Partitions.
			if _, ok := tmap[trole.TableKey]; !ok {
				continue
			}
			for _, partitionNum := range trole.Partitions {
				delete(tmap[trole.TableKey], partitionNum)
			}
		}
	}

	// Convert tmap back to d.TranslateRoles.
	troles := make([]TranslateRole, 0, len(tmap)+len(tmapf))
	for tkey, pmap := range tmap {
		partitions := make([]PartitionNum, 0, len(pmap))
		for p := range pmap {
			partitions = append(partitions, p)
		}
		sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
		troles = append(troles, TranslateRole{
			TableKey:   tkey,
			Partitions: partitions,
		})
	}
	for tkey, fmap := range tmapf {
		fields := make([]FieldName, 0, len(fmap))
		for f := range fmap {
			fields = append(fields, f)
		}
		sort.Slice(fields, func(i, j int) bool { return fields[i] < fields[j] })
		troles = append(troles, TranslateRole{
			TableKey: tkey,
			Fields:   fields,
		})
	}

	// Sort troles by table.
	sort.Slice(troles, func(i, j int) bool { return troles[i].TableKey < troles[j].TableKey })
	d.TranslateRoles = troles

	// It doesn't really matter that we set method on the directive to be
	// cached, but we do it just for informational purposes.
	d.Method = diff.Method

	// Finally, be sure to use the incoming version, not the version from d.
	d.Version = diff.Version

	return d
}

// Directives is a sortable slice of Directive.
type Directives []*Directive

func (d Directives) Len() int           { return len(d) }
func (d Directives) Less(i, j int) bool { return d[i].Address < d[j].Address }
func (d Directives) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
