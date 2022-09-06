package idk

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/bits"
	"net/http"
	"net/url"
	"time"
	"unicode/utf8"

	"sync"

	pilosacore "github.com/featurebasedb/featurebase/v3"

	"github.com/pkg/errors"
)

type RangeAllocator interface {
	Get() (*IDRange, error)
	Return(*IDRange) error
}

type IDAllocator interface {
	Next(context.Context, Record) (uint64, error)
	Reserve(context.Context, uint64) error
	Commit(context.Context) error
}

type LocalRangeAllocator struct {
	shardWidth uint64
	next       uint64
	returned   []*IDRange
	mu         sync.Mutex
}

func NewLocalRangeAllocator(shardWidth uint64) RangeAllocator {
	if shardWidth < 1<<16 || bits.OnesCount64(shardWidth) > 1 {
		panic(fmt.Sprintf("bad shardWidth in NewRangeAllocator: %d", shardWidth))
	}
	return &LocalRangeAllocator{
		shardWidth: shardWidth,
	}
}

// IDRange is inclusive at Start and exclusive at End... like slices.
type IDRange struct {
	Start uint64
	End   uint64
}

type rangeNexter struct {
	a RangeAllocator
	r *IDRange
}

func NewRangeNexter(a RangeAllocator) (IDAllocator, error) {
	r, err := a.Get()
	if err != nil {
		return nil, errors.Wrap(err, "getting range")
	}
	return &rangeNexter{
		a: a,
		r: r,
	}, nil
}

func (n *rangeNexter) Next(ctx context.Context, _ Record) (val uint64, err error) {
	if n.r.Start == n.r.End {
		n.r, err = n.a.Get()
		if err != nil {
			return 0, errors.Wrap(err, "getting next range")
		}
	}
	if n.r.Start >= n.r.End {
		panic("Start is greater than End")
	}
	val = n.r.Start
	n.r.Start++
	return
}

func (n *rangeNexter) Reserve(ctx context.Context, count uint64) error {
	return nil
}

func (n *rangeNexter) Commit(ctx context.Context) error {
	return nil
}

func (a *LocalRangeAllocator) Get() (*IDRange, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := len(a.returned)
	if n > 0 {
		ret := a.returned[n-1]
		a.returned = a.returned[:n-1]
		return ret, nil
	}
	ret := &IDRange{
		Start: a.next,
		End:   a.next + a.shardWidth,
	}
	a.next += a.shardWidth
	return ret, nil
}

func (a *LocalRangeAllocator) Return(r *IDRange) error {
	if r.Start == r.End {
		return nil
	}
	if r.Start > r.End {
		return errors.Errorf("attempted to return range with start > end: %v", r)
	}
	a.mu.Lock()
	a.returned = append(a.returned, r)
	a.mu.Unlock()
	return nil
}

type normalModeNexter struct {
	key         pilosacore.IDAllocKey
	session     [32]byte
	localIDs    []pilosacore.IDRange
	localOffset uint64
	idManager   *pilosaIDManager
}

func newNormalModeNexter(idManager *pilosaIDManager, index, key string) IDAllocator {
	nexter := &normalModeNexter{
		key: pilosacore.IDAllocKey{
			Index: index,
			Key:   key,
		},
		idManager: idManager,
	}
	return nexter
}

func (n *normalModeNexter) Reserve(ctx context.Context, count uint64) (err error) {
	n.localIDs = nil
	n.localOffset = 0
	_, err = io.ReadFull(rand.Reader, n.session[:])
	if err != nil {
		return errors.Wrap(err, "acquiring session ID")
	}
	reserveReq := pilosacore.IDAllocReserveRequest{
		Key:     n.key,
		Session: n.session,
		Offset:  ^uint64(0),
		Count:   count,
	}
	ids, err := n.idManager.reserve(ctx, reserveReq)
	if err != nil {
		return err
	}
	n.localIDs = ids
	return nil
}

func (n *normalModeNexter) Commit(ctx context.Context) error {
	commitRequest := pilosacore.IDAllocCommitRequest{
		Key:     n.key,
		Session: n.session,
		Count:   n.localOffset,
	}
	err := n.idManager.commit(ctx, commitRequest)
	if err != nil {
		return err
	}
	n.localIDs = nil
	n.localOffset = 0
	return nil
}

func (n *normalModeNexter) Next(ctx context.Context, _ Record) (uint64, error) {
	if len(n.localIDs) == 0 {
		return 0, errors.New("no reserved IDs")
	}

	id := n.localIDs[0].First
	if n.localIDs[0].First == n.localIDs[0].Last {
		n.localIDs = n.localIDs[1:]
	} else {
		n.localIDs[0].First++
	}
	n.localOffset++
	return id, nil
}

// idAllocation is a collection of IDs reserved for an offset stream.
type idAllocation struct {
	// localIDs are the currently-reserved IDs to use.
	// The first ID (if present) maps to offset.
	localIDs []pilosacore.IDRange

	// offset is the offset of the lowest available mapped record.
	offset uint64
}

// offsetModeNexter reserves IDs when pulling data from a set of offset-streams (e.g. kafka partitions).
// Currently, a lot of the design of offsetModeNexter assumes
// it's used with kafka though ideally, it could be used with
// any other source that's partitioned and offset based.
type offsetModeNexter struct {
	// idManager is a client to Pilosa's ID allocator.
	idManager *pilosaIDManager

	// index is the index to reserve IDs on.
	index string

	// reserveCount is the number of IDs to request from Pilosa at once.
	reserveCount uint64

	// localStore is the set of currently-reserved IDs.
	// When using kafka, the key is derived from the topic and partition.
	localStore map[string]*idAllocation
}

func newOffsetModeNexter(idManager *pilosaIDManager, index string, reserveCount uint64) IDAllocator {
	nexter := &offsetModeNexter{
		idManager:    idManager,
		index:        index,
		reserveCount: reserveCount,
		localStore:   make(map[string]*idAllocation),
	}
	return nexter
}

func (n *offsetModeNexter) Next(ctx context.Context, r Record) (uint64, error) {
	// Get the stream and offset of the record. In the case of a kafka message
	// the stream key will have the following format: "<topic>:<partition>"
	rec, ok := r.(OffsetStreamRecord)
	if !ok {
		return 0, errors.New("invalid source of records for OffsetMode ID Allocator: should implement OffsetStreamRecord")
	}
	key, offset := rec.StreamOffset()

	// retrieve allocation based on key
	// for kafka, the key is constructed from the topic + partition
	alloc, ok := n.localStore[key]
	if !ok {
		alloc = &idAllocation{}

		n.localStore[key] = alloc
	}

	if offset < alloc.offset {
		// This may happen in a brief period when two ingesters are processing the same partition.
		return 0, ErrIDOffsetDesync{
			Requested: offset,
			Base:      alloc.offset,
		}
	}

	// Skip IDs under the offset.
	if len(alloc.localIDs) != 0 && offset > alloc.offset {
		for len(alloc.localIDs) != 0 && alloc.localIDs[0].Last-alloc.localIDs[0].First < offset-alloc.offset {
			alloc.offset += alloc.localIDs[0].Last - alloc.localIDs[0].First + 1
			alloc.localIDs = alloc.localIDs[1:]
		}
		if len(alloc.localIDs) != 0 {
			skip := offset - alloc.offset
			alloc.offset += skip
			alloc.localIDs[0].First += skip
		}
	}

	// if we don't have any local IDs to nextify, then
	// get fresh IDs from idManager ie Pilosa
	if len(alloc.localIDs) == 0 {
		// get fresh IDs from Pilosa
		allocKey := pilosacore.IDAllocKey{
			Key:   key,
			Index: n.index,
		}
		var session [32]byte
		_, err := io.ReadFull(rand.Reader, session[:])
		if err != nil {
			return 0, errors.Wrap(err, "acquiring session ID")
		}
		reserveReq := pilosacore.IDAllocReserveRequest{
			Key:     allocKey,
			Session: session,
			Offset:  offset,
			Count:   n.reserveCount,
		}
		ids, err := n.idManager.reserve(ctx, reserveReq)
		if err != nil {
			if err, ok := err.(ErrIDOffsetDesync); ok {
				// Skip all messages until the latest offset.
				alloc.offset = err.Base
			}

			return 0, err
		}

		alloc.localIDs = ids
		alloc.offset = offset
	}

	// finally, get the ID
	return alloc.localIDs[0].First, nil
}

// Commits for offset-based allocated IDs are done implicitly when
// one reserves the next IDs, so there's no need to carry out an
// explicit commit here (for the time-being).
func (n *offsetModeNexter) Commit(ctx context.Context) error {
	return nil
}

func (n *offsetModeNexter) Reserve(ctx context.Context, count uint64) (err error) {
	// no op, reservation is done on demand during calls to Next
	return nil
}

// pilosaIDManager is an external ID Manager
// for autogenerated IDs
type pilosaIDManager struct {
	hcli *http.Client
	url  url.URL
}

// TODO, once pull/1559 is merged into pilosa main branch
// no need to maintain a duplicate definition of ErrIDOffsetDesync
// here
type ErrIDOffsetDesync struct {
	Requested uint64 `json:"requested"`
	// Base is the next lowest uncommitted offset for which
	// IDs may be reserved
	Base uint64 `json:"base"`
}

func (e ErrIDOffsetDesync) Error() string {
	return fmt.Sprintf("attempted to reserve IDs at committed offset %d (base offset: %d)", e.Requested, e.Base)
}

func (ps *pilosaIDManager) reserve(ctx context.Context, reserveReq pilosacore.IDAllocReserveRequest) ([]pilosacore.IDRange, error) {
	data, err := json.Marshal(reserveReq)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling ID reservation request")
	}
	targ, err := ps.url.Parse("/internal/idalloc/reserve")
	if err != nil {
		return nil, errors.Wrap(err, "resolving ID reservation target URL")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targ.String(), bytes.NewReader(data))
	if err != nil {
		return nil, errors.Wrap(err, "preparing ID reservation HTTP request")
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	// add authorization token to request
	token, ok := ctx.Value(contextKeyToken).(string)
	if ok && token != "" {
		req.Header.Set("Authorization", token)
	}

	resp, err := ps.hcli.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "dispatching ID reservation HTTP request")
	}
	var body []byte
	if resp.Body != nil {
		defer func() {
			cerr := resp.Body.Close()
			if cerr != nil && err == nil {
				err = errors.Wrap(cerr, "closing ID reservation request body")
			}
		}()
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.Wrap(err, "reading ID reservation request body")
		}
	}
	if resp.StatusCode/100 != http.StatusOK/100 {
		// first check if OffsetDesync Error
		if resp.StatusCode == http.StatusConflict && resp.Header.Get("Content-Type") == "application/json" {
			var esync ErrIDOffsetDesync
			if err := json.Unmarshal(body, &esync); err == nil {
				return nil, esync
			}
		}
		var err error
		if len(body) > 0 && utf8.Valid(body) {
			err = errors.New(string(body))
		} else {
			err = errors.New(resp.Status)
		}
		err = errors.Wrapf(err, "status %d", resp.StatusCode)
		return nil, errors.Wrap(err, "fetching ID reservation")
	}
	var ids []pilosacore.IDRange
	err = json.Unmarshal(body, &ids)
	if err != nil {
		return nil, errors.Wrap(err, "decoding ID reservation")
	}
	return ids, nil
}

func (ps *pilosaIDManager) commit(ctx context.Context, commitRequest pilosacore.IDAllocCommitRequest) error {
	data, err := json.Marshal(commitRequest)
	if err != nil {
		return errors.Wrap(err, "marshaling ID commit request")
	}
	targ, err := ps.url.Parse("/internal/idalloc/commit")
	if err != nil {
		return errors.Wrap(err, "resolving ID commit target URL")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targ.String(), bytes.NewReader(data))
	if err != nil {
		return errors.Wrap(err, "preparing ID commit HTTP request")
	}
	req.Header.Add("Content-Type", "application/json")

	// add authorization token to request
	token, ok := ctx.Value(contextKeyToken).(string)
	if ok && token != "" {
		req.Header.Set("Authorization", token)
	}

	resp, err := ps.hcli.Do(req)
	if err != nil {
		return errors.Wrap(err, "dispatching ID commit HTTP request")
	}
	var body []byte
	if resp.Body != nil {
		defer func() {
			cerr := resp.Body.Close()
			if cerr != nil && err == nil {
				err = errors.Wrap(cerr, "closing ID reservation request body")
			}
		}()
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrap(err, "reading ID reservation request body")
		}
	}
	if resp.StatusCode/100 != http.StatusOK/100 {
		var err error
		if len(body) > 0 && utf8.Valid(body) {
			err = errors.New(string(body))
		} else {
			err = errors.New(resp.Status)
		}
		err = errors.Wrapf(err, "status %d", resp.StatusCode)
		return errors.Wrap(err, "committing ID reservation")
	}

	return nil
}
