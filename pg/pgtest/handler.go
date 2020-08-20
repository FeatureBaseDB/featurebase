// Copyright 2020 Pilosa Corp.
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

package pgtest

import (
	"context"

	"github.com/pilosa/pilosa/v2/pg"
)

// HandlerFunc implements a postgres query handler with a function.
type HandlerFunc func(context.Context, pg.QueryResultWriter, pg.Query) error

// HandleQuery calls the user's query handler function.
func (h HandlerFunc) HandleQuery(ctx context.Context, w pg.QueryResultWriter, q pg.Query) error {
	return h(ctx, w, q)
}

var _ pg.QueryHandler = HandlerFunc(nil)
