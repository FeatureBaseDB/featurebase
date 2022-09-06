// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ingest

import (
	"github.com/featurebasedb/featurebase/v3/roaring"
)

// ShardUpdate is an update request for a shard.
type ShardUpdate struct {
	// TODO: include schema version

	// Sets are the updates to set fields.
	Sets map[string]SetMatrixUpdate

	// Mutexes are the updates to mutex fields.
	Mutexes map[string]MutexMatrixUpdate

	// TimeTensors are the updates to time fields.
	TimeTensors map[string]TimeTensorUpdate

	// Ints are the updates to int fields.
	Ints map[string]IntUpdate
}

// Convert the ID vector to a raw update that can be imported.
// All records are assumed to fall within a shard.
// func (vec IDVector) Convert() (MutexMatrixUpdate, error) {
// 	// Before converting the vector, do a sanity-check for duplicates.
// 	dedup := make(map[uint64]struct{}, len(vec.Updates)+len(vec.Clears))
// 	for _, p := range vec.Updates {
// 		if _, ok := dedup[p.RecordID]; !ok {
// 			return MutexMatrixUpdate{}, errors.New("input contains conflicting updates")
// 		}
//
// 		dedup[p.RecordID] = struct{}{}
// 	}
// 	for _, p := range vec.Clears {
// 		if _, ok := dedup[p]; !ok {
// 			return MutexMatrixUpdate{}, errors.New("input contains conflicting updates")
// 		}
//
// 		dedup[p] = struct{}{}
// 	}
//
// 	// Convert the updates to a bitmap.
// 	updates, err := idPairsToBitmap(vec.Updates, false)
// 	if err != nil {
// 		return MutexMatrixUpdate{}, errors.Wrap(err, "encoding mutex updates")
// 	}
//
// 	// Convert the clears to a bitmap.
// 	clears, err := idListToBitmap(vec.Clears, false)
// 	if err != nil {
// 		return MutexMatrixUpdate{}, errors.Wrap(err, "encoding mutex clears")
// 	}
//
// 	// Realign clears bitmap to start of shard.
// 	if min, ok := clears.Min(); ok {
// 		min &^= (1 << shardwidth.Exponent) - 1
// 		clears = clears.OffsetRange(-min, 0, 1<<shardwidth.Exponent)
// 	}
//
// 	return MutexMatrixUpdate{
// 		Update: updates,
// 		Clear:  clears,
// 	}, nil
// }

// MutexMatrixUpdate is an encoded update request for a mutex-type view.
type MutexMatrixUpdate struct {
	// Update is a bitmap of new mutex values.
	// This bitmap will not include any records in the clear bitmap.
	Update *roaring.Bitmap

	// Clear is a set of records for which all values should be removed.
	Clear *roaring.Bitmap
}

// Convert the time vectors to a raw update that can be imported.
// All records are assumed to fall within a shard.
// func (vec TimeIDSetsVector) Convert() (TimeTensorUpdate, error) {
// 	// Convert the clears to a bitmap.
// 	clears, err := idListToBitmap(vec.Clears, false)
// 	if err != nil {
// 		return TimeTensorUpdate{}, errors.Wrap(err, "encoding time tensor clears")
// 	}
//
// 	// Realign clears bitmap to start of shard.
// 	if min, ok := clears.Min(); ok {
// 		min &^= (1 << shardwidth.Exponent) - 1
// 		clears = clears.OffsetRange(-min, 0, 1<<shardwidth.Exponent)
// 	}
//
// 	// Convert the adds to bitmaps.
// 	quantums := make(map[string]*roaring.Bitmap)
// 	for t, vec := range vec.Quantums {
// 		adds, err := idPairsToBitmap(vec, true)
// 		if err != nil {
// 			return TimeTensorUpdate{}, errors.Wrap(err, "encoding time adds")
// 		}
//
// 		quantums[t] = adds
// 	}
//
// 	return TimeTensorUpdate{
// 		Quantums: quantums,
// 		Clear:    clears,
// 	}, nil
// }

// TimeTensorUpdate is an encoded update request for a time field within a shard.
type TimeTensorUpdate struct {
	// Quantums are the component set matrix adds, grouped by time quantum.
	Quantums map[string]*roaring.Bitmap

	// Clear is a set of records for which all values should be removed.
	// This must not overlap with any values in the remove or clear sections of any quantum.
	Clear *roaring.Bitmap
}

// Convert the ID set vector to a raw update that can be imported.
// All records are assumed to fall within a shard.
// func (vec IDSetVector) Convert() (SetMatrixUpdate, error) {
// 	// Convert the clears to a bitmap.
// 	clears, err := idListToBitmap(vec.Clears, false)
// 	if err != nil {
// 		return SetMatrixUpdate{}, errors.Wrap(err, "encoding set clears")
// 	}
//
// 	// Realign clears bitmap to start of shard.
// 	if min, ok := clears.Min(); ok {
// 		min &^= (1 << shardwidth.Exponent) - 1
// 		clears = clears.OffsetRange(-min, 0, 1<<shardwidth.Exponent)
// 	}
//
// 	// Convert the adds to a bitmap.
// 	adds, err := idPairsToBitmap(vec.Adds, false)
// 	if err != nil {
// 		return SetMatrixUpdate{}, errors.Wrap(err, "encoding set adds")
// 	}
//
// 	// Verify that the removes do not include the cleared records.
// 	if clears.Any() {
// 		for i := 0; i < len(vec.Removes); {
// 			recID := vec.Removes[i].RecordID
// 			if clears.Contains(recID) {
// 				return SetMatrixUpdate{}, errors.New("removed element duplicated with a clear")
// 			}
//
// 			i++
// 			for i < len(vec.Removes) && vec.Removes[i].RecordID == recID {
// 				i++
// 			}
// 		}
// 	}
//
// 	// Convert the removes to a bitmap.
// 	removes, err := idPairsToBitmap(vec.Removes, false)
// 	if err != nil {
// 		return SetMatrixUpdate{}, errors.Wrap(err, "encoding set removes")
// 	}
//
// 	// Check that no bits are both added and removed.
// 	if adds.IntersectionCount(removes) > 0 {
// 		return SetMatrixUpdate{}, errors.New("set adds and removes overlap")
// 	}
//
// 	return SetMatrixUpdate{
// 		Add:    adds,
// 		Remove: removes,
// 		Clear:  clears,
// 	}, nil
// }

// SetMatrixUpdate is an encoded update request for a set-type (set/mutex) view.
type SetMatrixUpdate struct {
	// Add is a bitmap to union the matrix against.
	Add *roaring.Bitmap

	// Remove is a bitmap to difference out of the matrix.
	// No values will be present in both add and remove.
	// This bitmap will not include any records in the clear bitmap.
	Remove *roaring.Bitmap

	// Clear is a set of records for which all values should be removed.
	// This is applied before add operations.
	Clear *roaring.Bitmap
}

// func idPairsToBitmap(pairs []IDPair, allowDup bool) (*roaring.Bitmap, error) {
// 	return idListToBitmap(pairsToZigZag(pairs), allowDup)
// }
//
// func idListToBitmap(ids []uint64, allowDup bool) (*roaring.Bitmap, error) {
// 	vals := dedupSort64(ids)
// 	if len(vals) != len(ids) && !allowDup {
// 		return nil, errors.New("input contains duplicate values")
// 	}
//
// 	// TODO: make the roaring package actually handle this well
// 	return roaring.NewBitmap(vals...), nil
// }

// Convert the int vector to a raw update that can be imported.
// All records are assumed to fall within a shard.
// func (vec IntVector) Convert() (IntUpdate, error) {
// 	// Convert the clears to a bitmap.
// 	clears, err := idListToBitmap(vec.Clears, false)
// 	if err != nil {
// 		return IntUpdate{}, errors.Wrap(err, "encoding int clears")
// 	}
//
// 	// Realign clears bitmap to start of shard.
// 	if min, ok := clears.Min(); ok {
// 		min &^= (1 << shardwidth.Exponent) - 1
// 		clears = clears.OffsetRange(-min, 0, 1<<shardwidth.Exponent)
// 	}
//
// 	// Convert updates to a bitmap.
// 	updates, err := idListToBitmap(intToBSI(vec.Updates), false)
// 	if err != nil {
// 		return IntUpdate{}, errors.Wrap(err, "encoding int updates")
// 	}
//
// 	// Check that no updated records are also being cleared.
// 	if updates.IntersectionCount(clears) > 0 {
// 		return IntUpdate{}, errors.New("update duplicated with a clear")
// 	}
//
// 	return IntUpdate{
// 		BSI:   updates,
// 		Clear: clears,
// 	}, nil
// }

// IntUpdate is an encoded update request for a BSI (int/timestamp/etc.) view.
type IntUpdate struct {
	// BSI is a bitmap of new BSI data to overwrite existing values.
	BSI *roaring.Bitmap

	// Clear is a set of records to assign null.
	Clear *roaring.Bitmap
}
