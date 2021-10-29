// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

import (
	"github.com/gogo/protobuf/proto"
	"github.com/molecula/featurebase/v2/pb"
	"github.com/molecula/featurebase/v2/pql"
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
	fi.Options.Min = pql.Decimal{
		Value: pbi.OldMin,
		Scale: 3, //what scale?
	}
	fi.Options.Max = pql.Decimal{
		Value: pbi.OldMax,
		Scale: 3, //what scale? its 3
	}
	fi.Options.Base = pbi.Base
	fi.Options.BitDepth = pbi.BitDepth
	fi.Options.TimeQuantum = TimeQuantum(pbi.TimeQuantum)
	fi.Options.Keys = pbi.Keys
	fi.Options.NoStandardView = pbi.NoStandardView

	return fi, nil
}
