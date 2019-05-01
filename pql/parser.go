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

package pql

import (
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/pkg/errors"
)

// timeFormat is the go-style time format used to parse string dates.
const timeFormat = "2006-01-02T15:04"

// error strings in the parser
const duplicateArgErrorMessage = "duplicate argument provided"
const intOutOfRangeError = "integer is not in signed 64-bit range"

// parser represents a parser for the PQL language.
type parser struct {
	r io.Reader
	//scanner *bufScanner
	PQL
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *parser {
	return &parser{
		r: r,
		//		scanner: newBufScanner(r),
	}
}

// ParseString parses s into a query.
func ParseString(s string) (*Query, error) {
	return NewParser(strings.NewReader(s)).Parse()
}

// Parse parses the next node in the query.
func (p *parser) Parse() (*Query, error) {
	buf, err := ioutil.ReadAll(p.r)
	if err != nil {
		return nil, errors.Wrap(err, "reading buffer to parse")
	}
	p.PQL = PQL{
		Buffer: string(buf),
	}
	p.Init()
	err = p.PQL.Parse()
	if err != nil {
		return nil, errors.Wrap(err, "parsing")
	}

	// Handle specific panics from the parser and return them as errors.
	var v interface{}
	func() {
		defer func() { v = recover() }()
		p.Execute()
	}()
	if v != nil {
		errorMessage, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected parser error of type %T: %[1]v", v)
		}
		if strings.HasPrefix(errorMessage, duplicateArgErrorMessage) || strings.HasPrefix(errorMessage, intOutOfRangeError) {
			return nil, fmt.Errorf("%s", v)
		} else {
			panic(v)
		}
	}
	for _, call := range p.Query.Calls {
		if call == nil {
			return nil, fmt.Errorf("unexpected nil Call in query's call list")
		}
		if err := call.CheckArgs(); err != nil {
			return nil, err
		}
	}

	return &p.Query, nil
}
