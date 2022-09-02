// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"os"
	"sort"
	"time"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// IDAllocKey is an ID allocation key.
type IDAllocKey struct {
	Index string `json:"index"`
	Key   string `json:"key,omitempty"`
}

type IDAllocReserveRequest struct {
	Key     IDAllocKey `json:"key"`
	Session [32]byte   `json:"session"`
	Offset  uint64     `json:"offset"`
	Count   uint64     `json:"count"`
}

type IDAllocCommitRequest struct {
	Key     IDAllocKey `json:"key"`
	Session [32]byte   `json:"session"`
	Count   uint64     `json:"count"`
}

func (k IDAllocKey) String() string {
	// This is a pretty printing format.
	// **DO NOT** attempt to parse this.
	return k.Index + ":" + k.Key
}

type idAllocator struct {
	db           *bolt.DB
	fsyncEnabled bool
}

type OpenIDAllocatorFunc func(path string, enableFsync bool) (*idAllocator, error) // whyyyyyyyyy

func OpenIDAllocator(path string, enableFsync bool) (*idAllocator, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second, NoSync: !enableFsync})
	if err != nil {
		return nil, err
	}
	return &idAllocator{db: db, fsyncEnabled: enableFsync}, nil
}

func (ida *idAllocator) Replace(reader io.Reader) error {
	newFile := ida.db.Path() + ".bak"
	liveFile := ida.db.Path()
	file, err := os.OpenFile(newFile, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil
	}
	_, err = io.Copy(file, reader)
	if err != nil {
		return err
	}
	file.Close()
	err = ida.db.Close()
	if err != nil {
		return err
	}
	err = os.Rename(liveFile, liveFile+".sav")
	if err != nil {
		return err
	}
	err = os.Rename(newFile, liveFile)
	if err != nil {
		err = os.Rename(liveFile+".sav", liveFile)
		return err
	} else {
		_ = os.Remove(liveFile + ".sav")
	}
	db, err := bolt.Open(liveFile, 0600, &bolt.Options{Timeout: 1 * time.Second, NoSync: !ida.fsyncEnabled})
	ida.db = db
	return err
}

func (ida *idAllocator) Close() error {
	if ida == nil || ida.db == nil {
		return nil
	}
	defer func() { ida.db = nil }()
	return ida.db.Close()
}

func (ida *idAllocator) WriteTo(w io.Writer) (int64, error) {
	if ida == nil || ida.db == nil {
		return 0, fmt.Errorf("idAllocator closed")
	}

	tx, err := ida.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	return tx.WriteTo(w)
}

// ErrIDOffsetDesync is an error generated when attempting to reserve IDs at a committed offset.
// This will typically happen when kafka partitions are moved between kafka ingesters - there may be a brief period in which 2 ingesters are processing the same messages at the same time.
// The ingester can resolve this by ignoring messages under base.
type ErrIDOffsetDesync struct {
	// Requested is the offset that the client attempted to reserve.
	Requested uint64 `json:"requested"`

	// Base is the next uncommitted offset for which IDs may be reserved.
	Base uint64 `json:"base"`
}

func (err ErrIDOffsetDesync) Error() string {
	return fmt.Sprintf("attempted to reserve IDs at committed offset %d (base offset: %d)", err.Requested, err.Base)
}

func (ida *idAllocator) reserve(key IDAllocKey, session [32]byte, offset, count uint64) ([]IDRange, error) {
	if session == [32]byte{} {
		return nil, errors.New("detected a broken session key")
	}

	var ranges []IDRange
	err := ida.db.Update(func(tx *bolt.Tx) error {
		// Find the bucket associated with the key.
		bkt, err := key.findBucket(tx)
		if err != nil {
			return err
		}

		// Fetch the old reservation.
		var res idReservation
		err = res.decode(bkt.Get([]byte(key.Key + "\x00")))
		if err != nil {
			return errors.Wrap(err, "decoding old reservation")
		}
		res.lock = session
		if offset != ^uint64(0) && offset != res.offset {
			// Offset control is in use and the offset differs.
			if offset < res.offset {
				// This probbably means that 2 clients are running at the same time.
				// This is fine - just tell the client that is behind what had been dealt with.
				return ErrIDOffsetDesync{
					Requested: offset,
					Base:      res.offset,
				}
			}

			// Roll the IDs forward.
			diff := offset - res.offset
			for diff > 0 && len(res.ranges) > 0 {
				if res.ranges[0].width() > diff-1 {
					res.ranges[0].First += diff
					break
				}

				diff -= res.ranges[0].width() + 1
				res.ranges = res.ranges[1:]
			}
		}
		res.offset = offset

		// Count already-reserved ranges.
		var preReserved uint64
		for _, r := range res.ranges {
			sum, overflow := bits.Add64(preReserved, r.width(), 1)
			if overflow != 0 {
				return errors.New("impossibly large reservation")
			}
			preReserved = sum
		}

		switch {
		case preReserved == count:
			// The client probbably failed while ingesting this.
			// Spit out the same ID ranges a second time.
			ranges = res.ranges

		case preReserved > count:
			// This could happen????
			ranges = make([]IDRange, 0, len(res.ranges))
			for _, r := range res.ranges {
				if count == 0 {
					break
				}

				if count-1 < r.width() {
					ranges = append(ranges, IDRange{
						First: r.First,
						Last:  r.First + count - 1,
					})
					break
				}

				ranges = append(ranges, r)
				count -= r.width() + 1
			}

		default:
			// Reserve additional IDs.
			reserved, err := doReserveIDs(bkt, count-preReserved)
			if err != nil {
				return err
			}

			reserved = append(res.ranges, reserved...)
			res.ranges = reserved
			ranges = reserved
		}

		// Save the updated reservation.
		encoded, err := res.encode()
		if err != nil {
			return errors.Wrap(err, "encoding updated ID reservation entry")
		}
		err = bkt.Put([]byte(key.Key+"\x00"), encoded)
		if err != nil {
			return errors.Wrap(err, "saving updated ID reservation entry")
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrapf(err, "acquiring IDs from key %s in session %v", key, session)
	}
	return ranges, nil
}

func (ida *idAllocator) commit(key IDAllocKey, session [32]byte, count uint64) error {
	if session == [32]byte{} {
		return errors.New("detected a broken session key")
	}

	err := ida.db.Update(func(tx *bolt.Tx) error {
		// Find the bucket associated with the key.
		bkt, err := key.findBucket(tx)
		if err != nil {
			return err
		}

		// Fetch the old reservation.
		prev := bkt.Get([]byte(key.Key + "\x00"))
		if prev == nil {
			// There is nothing to commit.
			return errors.New("nothing to commit")
		}
		var res idReservation
		err = res.decode(prev)
		if err != nil {
			return errors.Wrap(err, "decoding old reservation")
		}
		if res.lock != session {
			// This probbably means that there are multiple clients attempting to manipulate the same ID allocator key.
			return errors.New("lock collision")
		}

		if res.offset != ^uint64(0) {
			// Update the offset.
			if count > ^uint64(1)-res.offset {
				return errors.New("offset overflow")
			}
			res.offset += count
		}

		// Discard processed ID ranges.
		n := count
		for n > 0 && len(res.ranges) > 0 {
			r := &res.ranges[0]
			w := r.width()
			if n-1 < w {
				r.First += n
				n = 0
				break
			}

			n -= w + 1
			res.ranges = res.ranges[1:]
		}
		if n > 0 {
			// The client attempted to commit more IDs than they requested.
			return errors.New("overcommitted processed IDs")
		}

		if res.offset == ^uint64(0) && len(res.ranges) > 0 {
			// Return unused ranges to the central list.
			err = doReleaseIDs(bkt, res.ranges...)
			if err != nil {
				return errors.Wrap(err, "releasing unused IDs")
			}
			res.ranges = nil
		}

		if res.offset == ^uint64(0) && len(res.ranges) == 0 {
			// Delete the reservation entry, as it no longer contains any useful information.
			err = bkt.Delete([]byte(key.Key + "\x00"))
			if err != nil {
				return errors.Wrap(err, "deleting used ID reservation metadata")
			}
			return nil
		}

		// Save the updated reservation.
		encoded, err := res.encode()
		if err != nil {
			return errors.Wrap(err, "encoding updated ID reservation entry")
		}
		err = bkt.Put([]byte(key.Key+"\x00"), encoded)
		if err != nil {
			return errors.Wrap(err, "saving updated ID reservation entry")
		}

		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "committing %d IDs in allocation session %x with key %s", count, session, key)
	}
	return nil
}

func (ida *idAllocator) reset(index string) error {
	err := ida.db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte(index)) == nil {
			return nil
		}

		err := tx.DeleteBucket([]byte(index))
		if err != nil {
			return errors.Wrap(err, "deleting bucket")
		}

		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "resetting ID allocation for index %q", index)
	}
	return nil
}

func (k IDAllocKey) findBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	ibkt, err := tx.CreateBucketIfNotExists([]byte(k.Index))
	if err != nil {
		return nil, errors.Wrap(err, "creating index bucket")
	}
	return ibkt, nil
}

// IDRange is a reserved ID range.
type IDRange struct {
	First uint64 `json:"first"`
	Last  uint64 `json:"last"`
}

func (r *IDRange) decode(data []byte) error {
	if len(data) != 16 {
		return errors.New("wrong size for ID range")
	}
	first := binary.LittleEndian.Uint64(data[:8])
	last := binary.LittleEndian.Uint64(data[8:16])
	if last < first {
		return errors.New("backwards ID range")
	}
	*r = IDRange{First: first, Last: last}
	return nil
}

// width of the range (count-1).
func (r IDRange) width() uint64 {
	return r.Last - r.First
}

func decodeRanges(data []byte) ([]IDRange, error) {
	if len(data)%16 != 0 {
		return nil, errors.New("incorrectly sized ranges list")
	}

	ranges := make([]IDRange, len(data)/16)
	for i := range ranges {
		err := ranges[i].decode(data[16*i:][:16])
		if err != nil {
			return nil, errors.Wrapf(err, "decoding range %d", i)
		}
	}

	return ranges, nil
}

func encodeRanges(ranges []IDRange) ([]byte, error) {
	data := make([]byte, 16*len(ranges))
	for i, r := range ranges {
		if r.Last < r.First {
			return nil, errors.Errorf("backwards ID range at index %d", i)
		}
		rdat := data[16*i:][:16]
		binary.LittleEndian.PutUint64(rdat[0:8], r.First)
		binary.LittleEndian.PutUint64(rdat[8:16], r.Last)
	}
	return data, nil
}

type idReservation struct {
	ranges []IDRange
	offset uint64
	lock   [32]byte
}

func (r *idReservation) decode(data []byte) error {
	if data == nil {
		*r = idReservation{}
		return nil
	}

	if len(data) < 8+len(r.lock) {
		return errors.New("invalid ID reservation entry")
	}

	offset := binary.LittleEndian.Uint64(data[0:8])
	lock := data[8 : 8+len(r.lock)]

	ranges, err := decodeRanges(data[8+len(r.lock):])
	if err != nil {
		return errors.Wrap(err, "decoding ranges in reservation entry")
	}

	r.ranges = ranges
	r.offset = offset
	copy(r.lock[:], lock)

	return nil
}

func (r idReservation) encode() ([]byte, error) {
	var odat [8]byte
	binary.LittleEndian.PutUint64(odat[:], r.offset)
	rdat, err := encodeRanges(r.ranges)
	if err != nil {
		return nil, err
	}
	return append(append(odat[:], r.lock[:]...), rdat...), nil
}

func doReserveIDs(bkt *bolt.Bucket, count uint64) ([]IDRange, error) {
	var avail []IDRange
	if prev := bkt.Get([]byte{1}); prev != nil {
		r, err := decodeRanges(prev)
		if err != nil {
			return nil, err
		}
		avail = r
	} else {
		avail = []IDRange{{1, ^uint64(0)}}
	}
	reserved, avail, err := reserveIDs(avail, count)
	if err != nil {
		return nil, errors.Wrap(err, "reserving IDs")
	}
	encoded, err := encodeRanges(avail)
	if err != nil {
		return nil, errors.Wrap(err, "encoding available ID ranges")
	}
	err = bkt.Put([]byte{1}, encoded)
	if err != nil {
		return nil, errors.Wrap(err, "saving available ID ranges")
	}
	return reserved, nil
}

func reserveIDs(avail []IDRange, count uint64) (reserved []IDRange, unused []IDRange, err error) {
	// Min-heapify the available ranges by size.
	for i, v := range avail {
		width := v.width()
		for width < avail[(i-1)/2].width() {
			avail[(i-1)/2], avail[i] = v, avail[(i-1)/2]
			i = (i - 1) / 2
		}
	}

	// Reserve ranges in ascending size order.
	for count > 0 && len(avail) > 0 {
		if count-1 < avail[0].width() {
			// The smallest available range is larger than what is needed.
			reserved = append(reserved, IDRange{
				First: avail[0].First,
				Last:  avail[0].First + count - 1,
			})
			avail[0].First += count
			count = 0
			break
		}

		// Completely reserve the smallest range.
		count -= avail[0].width() + 1
		reserved = append(reserved, avail[0])
		avail[0] = avail[len(avail)-1]
		avail = avail[:len(avail)-1]
		i := 0
		for {
			min := i
			if l := 2*i + 1; l < len(avail) && avail[l].width() < avail[min].width() {
				min = l
			}
			if r := 2*i + 2; r < len(avail) && avail[r].width() < avail[min].width() {
				min = r
			}
			if min == i {
				break
			}
			avail[i], avail[min] = avail[min], avail[i]
			i = min
		}
	}
	if count > 0 {
		return nil, nil, errors.Errorf("insufficient available ID space (missing %d IDs)", count)
	}

	return reserved, avail, nil
}

func doReleaseIDs(bkt *bolt.Bucket, ranges ...IDRange) error {
	var avail []IDRange
	if prev := bkt.Get([]byte{1}); prev != nil {
		r, err := decodeRanges(prev)
		if err != nil {
			return err
		}
		avail = r
	} else {
		return errors.New("cannot return IDs to the void")
	}
	avail, err := mergeIDs(avail, ranges)
	if err != nil {
		return errors.Wrap(err, "returning IDs")
	}
	encoded, err := encodeRanges(avail)
	if err != nil {
		return errors.Wrap(err, "encoding available ID ranges")
	}
	err = bkt.Put([]byte{1}, encoded)
	if err != nil {
		return errors.Wrap(err, "saving available ID ranges")
	}
	return nil
}

func mergeIDs(main, returned []IDRange) ([]IDRange, error) {
	if len(main) == 0 && len(returned) == 0 {
		return nil, nil
	}

	main = append(main, returned...)

	sort.Slice(main, func(i, j int) bool { return main[i].First < main[j].First })

	i := 1
	for _, v := range main[1:] {
		prev := &main[i-1]
		switch {
		case v.First == prev.First || v.First <= prev.Last:
			return nil, errors.Errorf("found unexpected overlap between ranges %v and %v", prev, v)
		case v.First == prev.Last+1:
			prev.Last = v.Last
		default:
			main[i] = v
			i++
		}
	}

	return main[:i], nil
}
