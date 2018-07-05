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
	"io"
	"io/ioutil"
	"strings"

	"github.com/pkg/errors"
)

// TimeFormat is the go-style time format used to parse string dates.
const TimeFormat = "2006-01-02T15:04"

// Parser represents a parser for the PQL language.
type Parser struct {
	r io.Reader
	//scanner *bufScanner
	pQL
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{
		r: r,
		//		scanner: newBufScanner(r),
	}
}

// ParseString parses s into a query.
func ParseString(s string) (*Query, error) {
	return NewParser(strings.NewReader(s)).Parse()
}

// Parse parses the next node in the query.
func (p *Parser) Parse() (*Query, error) {
	buf, err := ioutil.ReadAll(p.r)
	if err != nil {
		return nil, errors.Wrap(err, "reading buffer to parse")
	}
	p.pQL = pQL{
		Buffer: string(buf),
	}
	p.Init()
	err = p.pQL.Parse()
	if err != nil {
		return nil, errors.Wrap(err, "parsing")
	}
	p.Execute()
	return &p.Query, nil
}
