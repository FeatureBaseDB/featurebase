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
