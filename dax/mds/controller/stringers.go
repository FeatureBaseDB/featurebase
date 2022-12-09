package controller

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
)

// pUnit represents a table/partition combination. As a Stringer, it can be
// used as a job in the Balancer.
type pUnit struct {
	t dax.TableKey
	p dax.VersionedPartition
}

func (p pUnit) String() string {
	return fmt.Sprintf("%s|part_%d", p.t, p.p.Num)
}

func (p pUnit) table() dax.TableKey {
	return p.t
}

func (p pUnit) partitionNum() dax.PartitionNum {
	return p.p.Num
}

func partition(t dax.TableKey, p dax.VersionedPartition) pUnit {
	return pUnit{t, p}
}

func partitions(t dax.TableKey, p ...dax.VersionedPartition) []pUnit {
	ret := make([]pUnit, 0, len(p))
	for _, vp := range p {
		ret = append(ret, pUnit{t, vp})
	}
	return ret
}

func decodePartition(j dax.Job) (pUnit, error) {
	s := string(j)
	parts := strings.Split(s, "|")
	if len(parts) != 2 {
		return pUnit{}, errors.Errorf("cannot decode string to partition: %s", s)
	}
	pparts := strings.Split(parts[1], "_")
	if len(pparts) != 2 {
		return pUnit{}, errors.Errorf("cannot decode partition part of string: %s", pparts[1])
	}
	intVar, err := strconv.Atoi(pparts[1])
	if err != nil {
		return pUnit{}, errors.Wrap(err, "converting string to int")
	}

	return pUnit{
		t: dax.TableKey(parts[0]),
		p: dax.VersionedPartition{
			Num:     dax.PartitionNum(intVar),
			Version: -1,
		},
	}, nil
}

// sUnit represents a table/shard combination. As a Stringer, it can be used as
// a job in the Balancer.
type sUnit struct {
	t dax.TableKey
	s dax.VersionedShard
}

func (s sUnit) String() string {
	return fmt.Sprintf("%s|shard_%s", s.t, s.s.Num)
}

func (s sUnit) table() dax.TableKey {
	return s.t
}

func (s sUnit) shardNum() dax.ShardNum {
	return s.s.Num
}

func shard(t dax.TableKey, s dax.VersionedShard) sUnit {
	return sUnit{t, s}
}

func decodeShard(j dax.Job) (sUnit, error) {
	s := string(j)
	parts := strings.Split(s, "|")
	if len(parts) != 2 {
		return sUnit{}, errors.Errorf("cannot decode string to shardV: %s", s)
	}
	pparts := strings.Split(parts[1], "_")
	if len(pparts) != 2 {
		return sUnit{}, errors.Errorf("cannot decode shard part of string: %s", pparts[1])
	}
	uint64Var, err := strconv.ParseUint(pparts[1], 10, 64)
	if err != nil {
		return sUnit{}, errors.Wrap(err, "converting string to int")
	}

	return sUnit{
		t: dax.TableKey(parts[0]),
		s: dax.VersionedShard{
			Num:     dax.ShardNum(uint64Var),
			Version: -1,
		},
	}, nil
}
