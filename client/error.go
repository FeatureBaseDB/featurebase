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

package client

import "github.com/pkg/errors"

// Predefined Pilosa errors.
var (
	ErrEmptyCluster                = errors.New("No usable addresses in the cluster")
	ErrIndexExists                 = errors.New("Index exists")
	ErrFieldExists                 = errors.New("Field exists")
	ErrInvalidIndexName            = errors.New("Invalid index name")
	ErrInvalidFieldName            = errors.New("Invalid field name")
	ErrInvalidLabel                = errors.New("Invalid label")
	ErrInvalidKey                  = errors.New("Invalid key")
	ErrTriedMaxHosts               = errors.New("Tried max hosts, still failing")
	ErrAddrURIClusterExpected      = errors.New("Addresses, URIs or a cluster is expected")
	ErrInvalidQueryOption          = errors.New("Invalid query option")
	ErrInvalidIndexOption          = errors.New("Invalid index option")
	ErrInvalidFieldOption          = errors.New("Invalid field option")
	ErrNoFragmentNodes             = errors.New("No fragment nodes")
	ErrNoShard                     = errors.New("Index has no shards")
	ErrUnknownType                 = errors.New("Unknown type")
	ErrSingleServerAddressRequired = errors.New("OptClientManualServerAddress requires a single URI or address")
	ErrPreconditionFailed          = errors.New("Precondition failed")
)
