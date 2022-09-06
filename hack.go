// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"math"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/featurebasedb/featurebase/v3/pb"
	"github.com/featurebasedb/featurebase/v3/pql"
)

func UnmarshalIndexOptions(name string, createdAt int64, buf []byte) (*IndexOptions, error) {
	var io pb.IndexMeta
	// Read data from meta file.
	if err := proto.Unmarshal(buf, &io); err != nil {
		return nil, err
	}
	return &IndexOptions{
		Keys:           io.Keys,
		TrackExistence: io.TrackExistence,
	}, nil

}
func UnmarshalFieldOptions(name string, createdAt int64, buf []byte) (*FieldInfo, error) {
	var pbi pb.FieldOptions

	// Read data from meta file.
	if err := proto.Unmarshal(buf, &pbi); err != nil {
		return nil, err
	}
	fi := &FieldInfo{}
	fi.Name = name
	fi.CreatedAt = createdAt
	fi.Options = FieldOptions{}
	// Initialize "base" to "min" when upgrading from v1 BSI format.
	if pbi.BitDepth == 0 {
		pbi.Base = bsiBase(pbi.OldMin, pbi.OldMax)
		pbi.BitDepth = uint64(bitDepthInt64(pbi.OldMax - pbi.OldMin))
		if pbi.BitDepth == 0 {
			pbi.BitDepth = 1
		}
	}

	// Copy metadata fields.
	fi.Options.Type = pbi.Type
	if pbi.Type == "decimal" {
		fi.Options.Scale = 3
	}
	fi.Options.CacheType = pbi.CacheType
	fi.Options.CacheSize = pbi.CacheSize
	fi.Options.Base = pbi.Base
	fi.Options.BitDepth = pbi.BitDepth
	fi.Options.TimeQuantum = TimeQuantum(pbi.TimeQuantum)
	ttlValue, err := time.ParseDuration(pbi.TTL)
	if err != nil {
		ttlValue = 0
	}
	fi.Options.TTL = ttlValue
	fi.Options.Keys = pbi.Keys
	fi.Options.NoStandardView = pbi.NoStandardView

	// hard code for the unmarshal go broken with a version change
	switch fi.Options.Type {
	case "int":
		min := int64(math.MinInt64)
		max := int64(math.MaxInt64)
		fi.Options.Min = pql.NewDecimal(min, 0)
		fi.Options.Max = pql.NewDecimal(max, 0)
		fi.Options.Base = 0
	case "decimal":
		scale := int64(3)
		min, max := pql.MinMax(scale)
		fi.Options.Min = min
		fi.Options.Max = max
		fi.Options.Base = 0
		fi.Options.Scale = scale
	}

	return fi, nil
}
