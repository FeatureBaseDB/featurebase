package controller

import (
	"sort"

	"github.com/featurebasedb/featurebase/v3/dax"
)

// StringSet is a set of strings.
type StringSet map[string]struct{}

func NewStringSet() StringSet {
	return make(StringSet)
}

func (s StringSet) Add(p string) {
	s[p] = struct{}{}
}

func (s StringSet) Remove(p string) {
	delete(s, p)
}

func (s StringSet) Contains(p string) bool {
	_, ok := s[p]
	return ok
}

func (s StringSet) SortedSlice() []string {
	ps := make([]string, 0, len(s))
	for p := range s {
		ps = append(ps, p)
	}
	sort.Strings(ps)

	return ps
}

func (s StringSet) Minus(m StringSet) []string {
	diff := []string{}

	for sk := range s {
		var found bool
		for mk := range m {
			if mk == sk {
				found = true
				break
			}
		}
		if !found {
			diff = append(diff, sk)
		}
	}

	return diff
}

// TableSet is a set of strings.
type TableSet map[dax.TableKey]struct{}

func NewTableSet() TableSet {
	return make(TableSet)
}

func (s TableSet) Add(t dax.TableKey) {
	s[t] = struct{}{}
}

func (s TableSet) Remove(t dax.TableKey) {
	delete(s, t)
}

func (s TableSet) Contains(t dax.TableKey) bool {
	_, ok := s[t]
	return ok
}

func (s TableSet) SortedSlice() dax.TableKeys {
	ps := make(dax.TableKeys, 0, len(s))
	for p := range s {
		ps = append(ps, p)
	}
	sort.Sort(ps)

	return ps
}

func (s TableSet) QualifiedSortedSlice() map[dax.QualifiedDatabaseID]dax.TableIDs {
	m := make(map[dax.QualifiedDatabaseID]dax.TableIDs)
	for p := range s {
		qtid := p.QualifiedTableID()
		m[qtid.QualifiedDatabaseID] = append(m[qtid.QualifiedDatabaseID], qtid.ID)
	}

	// Sort the slices in the map.
	for _, v := range m {
		sort.Sort(v)
	}

	return m
}

func (s TableSet) Minus(m TableSet) dax.TableKeys {
	diff := dax.TableKeys{}

	for sk := range s {
		var found bool
		for mk := range m {
			if mk == sk {
				found = true
				break
			}
		}
		if !found {
			diff = append(diff, sk)
		}
	}

	return diff
}

// AddressSet is a set of strings.
type AddressSet map[dax.Address]struct{}

func NewAddressSet() AddressSet {
	return make(AddressSet)
}

func (s AddressSet) Add(p dax.Address) {
	s[p] = struct{}{}
}

func (s AddressSet) Merge(o AddressSet) {
	for k := range o {
		s[k] = struct{}{}
	}
}

func (s AddressSet) Remove(p dax.Address) {
	delete(s, p)
}

func (s AddressSet) Contains(p dax.Address) bool {
	_, ok := s[p]
	return ok
}

func (s AddressSet) SortedSlice() []dax.Address {
	ps := make([]dax.Address, 0, len(s))
	for p := range s {
		ps = append(ps, p)
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })

	return ps
}

func (s AddressSet) Minus(m AddressSet) []dax.Address {
	diff := []dax.Address{}

	for sk := range s {
		var found bool
		for mk := range m {
			if mk == sk {
				found = true
				break
			}
		}
		if !found {
			diff = append(diff, sk)
		}
	}

	return diff
}
