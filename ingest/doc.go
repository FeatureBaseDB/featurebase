// Copyright 2021 Molecula Corp.
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

// Package ingest provides tooling for accepting record-oriented data updates
// and converting them to data that can be efficiently merged into stored
// data. Nia's original description:
//
// but the overall pipeline is:
// 1. fetch the schema and use it to configure the codec
// 2. parse the data with the codec into vectors, while stuffing temp record key mappings into a string table
// 3. call *CreateKeys on the cluster for all of the things
// 4. generate an ID remapping table for record keys and apply it to all of the vectors
// 5. remap the string keys
// 6. group each vector by shard
// 7. convert the shard vectors into matrix updates
// 8. combine those matrix updates into a shard update
// 9. send the shard updates out over the internal client
// 10. the nodes apply them to RBF
package ingest
