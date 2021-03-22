// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package client

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/pilosa/pilosa/v2/internal"
)

// QueryResponse types.
const (
	QueryResultTypeNil uint32 = iota
	QueryResultTypeRow
	QueryResultTypePairs
	QueryResultTypePairsField
	QueryResultTypeValCount
	QueryResultTypeUint64
	QueryResultTypeBool
	QueryResultTypeRowIDs // this is not used by the client
	QueryResultTypeGroupCounts
	QueryResultTypeRowIdentifiers
	QueryResultTypePair
	QueryResultTypePairField
	QueryResultTypeSignedRow
)

// QueryResponse represents the response from a Pilosa query.
type QueryResponse struct {
	ResultList   []QueryResult `json:"results,omitempty"`
	ColumnList   []ColumnItem  `json:"columns,omitempty"`
	ErrorMessage string        `json:"error-message,omitempty"`
	Success      bool          `json:"success,omitempty"`
}

func newQueryResponseFromInternal(response *internal.QueryResponse) (*QueryResponse, error) {
	if response.Err != "" {
		return &QueryResponse{
			ErrorMessage: response.Err,
			Success:      false,
		}, nil
	}
	results := make([]QueryResult, 0, len(response.Results))
	for _, r := range response.Results {
		result, err := newQueryResultFromInternal(r)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	columns := make([]ColumnItem, 0, len(response.ColumnAttrSets))
	for _, p := range response.ColumnAttrSets {
		columnItem, err := newColumnItemFromInternal(p)
		if err != nil {
			return nil, err
		}
		columns = append(columns, columnItem)
	}

	return &QueryResponse{
		ResultList: results,
		ColumnList: columns,
		Success:    true,
	}, nil
}

// Results returns all results in the response.
func (qr *QueryResponse) Results() []QueryResult {
	return qr.ResultList
}

// Result returns the first result or nil.
func (qr *QueryResponse) Result() QueryResult {
	if len(qr.ResultList) == 0 {
		return nil
	}
	return qr.ResultList[0]
}

// Columns returns all column attributes in the response.
// *DEPRECATED*
func (qr *QueryResponse) Columns() []ColumnItem {
	return qr.ColumnList
}

// Column returns the attributes for first column.
// *DEPRECATED*
func (qr *QueryResponse) Column() ColumnItem {
	if len(qr.ColumnList) == 0 {
		return ColumnItem{}
	}
	return qr.ColumnList[0]
}

// ColumnAttrs returns all column attributes in the response.
func (qr *QueryResponse) ColumnAttrs() []ColumnItem {
	return qr.ColumnList
}

// QueryResult represents one of the results in the response.
type QueryResult interface {
	Type() uint32
	Row() RowResult
	CountItems() []CountResultItem
	CountItem() CountResultItem
	Count() int64
	Value() int64
	Changed() bool
	GroupCounts() []GroupCount
	RowIdentifiers() RowIdentifiersResult
}

func newQueryResultFromInternal(result *internal.QueryResult) (QueryResult, error) {
	switch result.Type {
	case QueryResultTypeNil:
		return NilResult{}, nil
	case QueryResultTypeRow:
		return newRowResultFromInternal(result.Row)
	case QueryResultTypePairs:
		return countItemsFromInternal(result.Pairs), nil
	case QueryResultTypePairsField:
		return countItemsFromInternal(result.PairsField.Pairs), nil
	case QueryResultTypeValCount:
		return &ValCountResult{
			Val: result.ValCount.Val,
			Cnt: result.ValCount.Count,
		}, nil
	case QueryResultTypeUint64:
		return IntResult(result.N), nil
	case QueryResultTypeBool:
		return BoolResult(result.Changed), nil
	case QueryResultTypeRowIdentifiers:
		return &RowIdentifiersResult{
			IDs:  result.RowIdentifiers.Rows,
			Keys: result.RowIdentifiers.Keys,
		}, nil
	case QueryResultTypeGroupCounts:
		return groupCountsFromInternal(result.GroupCounts), nil
	case QueryResultTypePair:
		return CountItem{CountResultItem: countItemFromInternal(result.Pairs[0])}, nil
	case QueryResultTypePairField:
		return CountItem{CountResultItem: countItemFromInternal(result.PairField.Pair)}, nil
	}

	return nil, ErrUnknownType
}

// CountResultItem represents a result from TopN call.
type CountResultItem struct {
	ID    uint64 `json:"id"`
	Key   string `json:"key,omitempty"`
	Count uint64 `json:"count"`
}

func (c *CountResultItem) String() string {
	if c.Key != "" {
		return fmt.Sprintf("%s:%d", c.Key, c.Count)
	}
	return fmt.Sprintf("%d:%d", c.ID, c.Count)
}

type CountItem struct {
	CountResultItem
}

// Type is the type of this result.
func (CountItem) Type() uint32 { return QueryResultTypePairField }

// Row returns a RowResult.
func (CountItem) Row() RowResult { return RowResult{} }

// CountItems returns a CountResultItem slice.
func (t CountItem) CountItems() []CountResultItem { return []CountResultItem{t.CountResultItem} }

// CountItem returns a CountResultItem
func (t CountItem) CountItem() CountResultItem { return t.CountResultItem }

// Count returns the result of a Count call.
func (CountItem) Count() int64 { return 0 }

// Value returns the result of a Min, Max or Sum call.
func (CountItem) Value() int64 { return 0 }

// Changed returns whether the corresponding Set or Clear call changed the value of a bit.
func (CountItem) Changed() bool { return false }

// GroupCounts returns the result of a GroupBy call.
func (CountItem) GroupCounts() []GroupCount { return nil }

// RowIdentifiers returns the result of a Rows call.
func (CountItem) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

func countItemFromInternal(item *internal.Pair) CountResultItem {
	return CountResultItem{ID: item.ID, Key: item.Key, Count: item.Count}
}

func countItemsFromInternal(items []*internal.Pair) TopNResult {
	result := make([]CountResultItem, 0, len(items))
	for _, v := range items {
		result = append(result, countItemFromInternal(v))
	}
	return TopNResult(result)
}

// TopNResult is returned from TopN call.
type TopNResult []CountResultItem

// Type is the type of this result.
func (TopNResult) Type() uint32 { return QueryResultTypePairsField }

// Row returns a RowResult.
func (TopNResult) Row() RowResult { return RowResult{} }

// CountItems returns a CountResultItem slice.
func (t TopNResult) CountItems() []CountResultItem { return t }

// CountItem returns a CountResultItem
func (t TopNResult) CountItem() CountResultItem {
	if len(t) >= 1 {
		return t[0]
	}
	return CountResultItem{}
}

// Count returns the result of a Count call.
func (TopNResult) Count() int64 { return 0 }

// Value returns the result of a Min, Max or Sum call.
func (TopNResult) Value() int64 { return 0 }

// Changed returns whether the corresponding Set or Clear call changed the value of a bit.
func (TopNResult) Changed() bool { return false }

// GroupCounts returns the result of a GroupBy call.
func (TopNResult) GroupCounts() []GroupCount { return nil }

// RowIdentifiers returns the result of a Rows call.
func (TopNResult) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

// RowResult represents a result from Row, Union, Intersect, Difference and Range PQL calls.
type RowResult struct {
	Attributes map[string]interface{} `json:"attrs"`
	Columns    []uint64               `json:"columns"`
	Keys       []string               `json:"keys"`
}

func newRowResultFromInternal(row *internal.Row) (*RowResult, error) {
	attrs, err := convertInternalAttrsToMap(row.Attrs)
	if err != nil {
		return nil, err
	}
	result := &RowResult{
		Attributes: attrs,
		Columns:    row.Columns,
		Keys:       row.Keys,
	}
	return result, nil
}

// Type is the type of this result.
func (RowResult) Type() uint32 { return QueryResultTypeRow }

// Row returns a RowResult.
func (b RowResult) Row() RowResult { return b }

// CountItems returns a CountResultItem slice.
func (RowResult) CountItems() []CountResultItem { return nil }

// CountItem returns a CountResultItem
func (RowResult) CountItem() CountResultItem { return CountResultItem{} }

// Count returns the result of a Count call.
func (RowResult) Count() int64 { return 0 }

// Value returns the result of a Min, Max or Sum call.
func (RowResult) Value() int64 { return 0 }

// Changed returns whether the corresponding Set or Clear call changed the value of a bit.
func (RowResult) Changed() bool { return false }

// GroupCounts returns the result of a GroupBy call.
func (RowResult) GroupCounts() []GroupCount { return nil }

// RowIdentifiers returns the result of a Rows call.
func (RowResult) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

// MarshalJSON serializes this row result.
func (b RowResult) MarshalJSON() ([]byte, error) {
	columns := b.Columns
	if columns == nil {
		columns = []uint64{}
	}
	keys := b.Keys
	if keys == nil {
		keys = []string{}
	}
	return json.Marshal(struct {
		Attributes map[string]interface{} `json:"attrs"`
		Columns    []uint64               `json:"columns"`
		Keys       []string               `json:"keys"`
	}{
		Attributes: b.Attributes,
		Columns:    columns,
		Keys:       keys,
	})
}

// ValCountResult is returned from Min, Max and Sum calls.
type ValCountResult struct {
	Val int64 `json:"val"`
	Cnt int64 `json:"count"`
}

// Type is the type of this result.
func (ValCountResult) Type() uint32 { return QueryResultTypeValCount }

// Row returns a RowResult.
func (ValCountResult) Row() RowResult { return RowResult{} }

// CountItems returns a CountResultItem slice.
func (ValCountResult) CountItems() []CountResultItem { return nil }

// CountItem returns a CountResultItem
func (ValCountResult) CountItem() CountResultItem { return CountResultItem{} }

// Count returns the result of a Count call.
func (c ValCountResult) Count() int64 { return c.Cnt }

// Value returns the result of a Min, Max or Sum call.
func (c ValCountResult) Value() int64 { return c.Val }

// Changed returns whether the corresponding Set or Clear call changed the value of a bit.
func (ValCountResult) Changed() bool { return false }

// GroupCounts returns the result of a GroupBy call.
func (ValCountResult) GroupCounts() []GroupCount { return nil }

// RowIdentifiers returns the result of a Rows call.
func (ValCountResult) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

// IntResult is returned from Count call.
type IntResult int64

// Type is the type of this result.
func (IntResult) Type() uint32 { return QueryResultTypeUint64 }

// Row returns a RowResult.
func (IntResult) Row() RowResult { return RowResult{} }

// CountItems returns a CountResultItem slice.
func (IntResult) CountItems() []CountResultItem { return nil }

// CountItem returns a CountResultItem
func (IntResult) CountItem() CountResultItem { return CountResultItem{} }

// Count returns the result of a Count call.
func (i IntResult) Count() int64 { return int64(i) }

// Value returns the result of a Min, Max or Sum call.
func (IntResult) Value() int64 { return 0 }

// Changed returns whether the corresponding Set or Clear call changed the value of a bit.
func (IntResult) Changed() bool { return false }

// GroupCounts returns the result of a GroupBy call.
func (IntResult) GroupCounts() []GroupCount { return nil }

// RowIdentifiers returns the result of a Rows call.
func (IntResult) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

// BoolResult is returned from Set and Clear calls.
type BoolResult bool

// Type is the type of this result.
func (BoolResult) Type() uint32 { return QueryResultTypeBool }

// Row returns a RowResult.
func (BoolResult) Row() RowResult { return RowResult{} }

// CountItems returns a CountResultItem slice.
func (BoolResult) CountItems() []CountResultItem { return nil }

// CountItem returns a CountResultItem
func (BoolResult) CountItem() CountResultItem { return CountResultItem{} }

// Count returns the result of a Count call.
func (BoolResult) Count() int64 { return 0 }

// Value returns the result of a Min, Max or Sum call.
func (BoolResult) Value() int64 { return 0 }

// Changed returns whether the corresponding Set or Clear call changed the value of a bit.
func (b BoolResult) Changed() bool { return bool(b) }

// GroupCounts returns the result of a GroupBy call.
func (BoolResult) GroupCounts() []GroupCount { return nil }

// RowIdentifiers returns the result of a Rows call.
func (BoolResult) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

// NilResult is returned from calls which don't return a value, such as SetRowAttrs.
type NilResult struct{}

// Type is the type of this result.
func (NilResult) Type() uint32 { return QueryResultTypeNil }

// Row returns a RowResult.
func (NilResult) Row() RowResult { return RowResult{} }

// CountItems returns a CountResultItem slice.
func (NilResult) CountItems() []CountResultItem { return nil }

// CountItem returns a CountResultItem
func (NilResult) CountItem() CountResultItem { return CountResultItem{} }

// Count returns the result of a Count call.
func (NilResult) Count() int64 { return 0 }

// Value returns the result of a Min, Max or Sum call.
func (NilResult) Value() int64 { return 0 }

// Changed returns whether the corresponding Set or Clear call changed the value of a bit.
func (NilResult) Changed() bool { return false }

// GroupCounts returns the result of a GroupBy call.
func (NilResult) GroupCounts() []GroupCount { return nil }

// RowIdentifiers returns the result of a Rows call.
func (NilResult) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

// FieldRow represents a Group in a GroupBy call result.
type FieldRow struct {
	FieldName string `json:"field"`
	RowID     uint64 `json:"rowID"`
	RowKey    string `json:"rowKey"`
	Value     *int64 `json:"value,omitempty"`
}

// GroupCount contains groups and their count in a GroupBy call result.
type GroupCount struct {
	Groups []FieldRow `json:"groups"`
	Count  int64      `json:"count"`
	Agg    int64      `json:"agg"`
}

// GroupCountResult is returned from GroupBy call.
type GroupCountResult []GroupCount

// Type is the type of this result.
func (GroupCountResult) Type() uint32 { return QueryResultTypeGroupCounts }

// Row returns a RowResult.
func (GroupCountResult) Row() RowResult { return RowResult{} }

// CountItems returns a CountResultItem slice.
func (GroupCountResult) CountItems() []CountResultItem { return nil }

// CountItem returns a CountResultItem
func (GroupCountResult) CountItem() CountResultItem { return CountResultItem{} }

// Count returns the result of a Count call.
func (GroupCountResult) Count() int64 { return 0 }

// Value returns the result of a Min, Max or Sum call.
func (GroupCountResult) Value() int64 { return 0 }

// Changed returns whether the corresponding Set or Clear call changed the value of a bit.
func (GroupCountResult) Changed() bool { return false }

// GroupCounts returns the result of a GroupBy call.
func (r GroupCountResult) GroupCounts() []GroupCount { return r }

// RowIdentifiers returns the result of a Rows call.
func (GroupCountResult) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

// RowIdentifiersResult is returned from a Rows call.
type RowIdentifiersResult struct {
	IDs  []uint64 `json:"ids"`
	Keys []string `json:"keys,omitempty"`
}

// Type is the type of this result.
func (RowIdentifiersResult) Type() uint32 { return QueryResultTypeRowIdentifiers }

// Row returns a RowResult.
func (RowIdentifiersResult) Row() RowResult { return RowResult{} }

// CountItems returns a CountResultItem slice.
func (RowIdentifiersResult) CountItems() []CountResultItem { return nil }

// CountItem returns a CountResultItem
func (RowIdentifiersResult) CountItem() CountResultItem { return CountResultItem{} }

// Count returns the result of a Count call.
func (RowIdentifiersResult) Count() int64 { return 0 }

// Value returns the result of a Min, Max or Sum call.
func (RowIdentifiersResult) Value() int64 { return 0 }

// Changed returns whether the corresponding Set or Clear call changed the value of a bit.
func (RowIdentifiersResult) Changed() bool { return false }

// GroupCounts returns the result of a GroupBy call.
func (RowIdentifiersResult) GroupCounts() []GroupCount { return nil }

// RowIdentifiers returns the result of a Rows call.
func (r RowIdentifiersResult) RowIdentifiers() RowIdentifiersResult { return r }

func groupCountsFromInternal(items *internal.GroupCounts) GroupCountResult {
	result := make([]GroupCount, 0, len(items.Groups))
	for _, g := range items.Groups {
		groups := make([]FieldRow, 0, len(g.Group))
		for _, f := range g.Group {
			fr := FieldRow{
				FieldName: f.Field,
				RowID:     f.RowID,
				RowKey:    f.RowKey,
			}
			if f.Value != nil {
				fr.Value = &f.Value.Value
			}
			groups = append(groups, fr)
		}
		result = append(result, GroupCount{
			Groups: groups,
			Count:  int64(g.Count),
			Agg:    int64(g.Agg),
		})
	}
	return GroupCountResult(result)
}

const (
	stringType = 1
	intType    = 2
	boolType   = 3
	floatType  = 4
)

func convertInternalAttrsToMap(attrs []*internal.Attr) (attrsMap map[string]interface{}, err error) {
	attrsMap = make(map[string]interface{}, len(attrs))
	for _, attr := range attrs {
		switch attr.Type {
		case stringType:
			attrsMap[attr.Key] = attr.StringValue
		case intType:
			attrsMap[attr.Key] = attr.IntValue
		case boolType:
			attrsMap[attr.Key] = attr.BoolValue
		case floatType:
			attrsMap[attr.Key] = attr.FloatValue
		default:
			return nil, errors.New("Unknown attribute type")
		}
	}

	return attrsMap, nil
}

// ColumnItem represents data about a column.
// Column data is only returned if QueryOptions.Columns was set to true.
type ColumnItem struct {
	ID         uint64                 `json:"id,omitempty"`
	Key        string                 `json:"key,omitempty"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

func newColumnItemFromInternal(column *internal.ColumnAttrSet) (ColumnItem, error) {
	attrs, err := convertInternalAttrsToMap(column.Attrs)
	if err != nil {
		return ColumnItem{}, err
	}
	return ColumnItem{
		ID:         column.ID,
		Key:        column.Key,
		Attributes: attrs,
	}, nil
}
