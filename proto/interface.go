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
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StreamClient is an interface for a stream
// which can return a RowResponse sent to a
// stream via Send().
type StreamClient interface {
	Recv() (*RowResponse, error)
}

// StreamServer is an interface for a stream
// which can accept a RowResponse to be later
// returned by the stream via Recv().
type StreamServer interface {
	Send(*RowResponse) error
}

// EOF acts as an io.EOF encoded into a RowResponse.
var EOF *RowResponse = &RowResponse{
	StatusError: &StatusError{
		Code:    0,
		Message: "EOF",
	},
}

// Error is a helper function to create a RowResponse
// based on an error message. If the error is a grpc
// Status, then the status code is passed through.
func Error(err error) *RowResponse {
	status, _ := status.FromError(err)
	return &RowResponse{
		StatusError: &StatusError{
			Code:    uint32(status.Code()),
			Message: status.Err().Error(),
		},
	}
}

// ErrorWrap prepends a message to the existing status
// error message.
func ErrorWrap(err error, message string) *RowResponse {
	status, _ := status.FromError(err)
	return &RowResponse{
		StatusError: &StatusError{
			Code:    uint32(status.Code()),
			Message: message + ": " + status.Err().Error(),
		},
	}
}

// ErrorWrapf prepends a message to the existing status
// error message with the format specifier.
func ErrorWrapf(err error, format string, args ...interface{}) *RowResponse {
	status, _ := status.FromError(err)
	return &RowResponse{
		StatusError: &StatusError{
			Code:    uint32(status.Code()),
			Message: fmt.Sprintf(format, args...) + ": " + status.Err().Error(),
		},
	}
}

// ErrorCode is a helper function to create a RowResponse
// based on a grpc status code and an error message.
func ErrorCode(err error, c codes.Code) *RowResponse {
	return &RowResponse{
		StatusError: &StatusError{
			Code:    uint32(c),
			Message: err.Error(),
		},
	}
}

// RowResponseSorter implements the sort interface for a
// provided []RowResponse based on the column index, type,
// and sort direction.
type RowResponseSorter struct {
	colIdx        []int
	colDescending []bool
	colType       []string

	rrs []*RowResponse
}

// NewRowResponseSorter return a new RowResponseSorter. It
// does input validation and returns an error if the inputs
// aren't compatible.
func NewRowResponseSorter(idxs []int, dirs []bool, typs []string, rrs []*RowResponse) (*RowResponseSorter, error) {
	// Ensure the input slices are non-empty and equal size.
	if len(idxs) == 0 {
		return nil, errors.New("index list cannot be empty")
	}
	if len(dirs) != len(idxs) || len(typs) != len(idxs) {
		return nil, errors.New("index, direction, and type lists must be the same size")
	}

	// Ensure the provided data types are supported by the sorter.
	for i := range typs {
		switch typs[i] {
		case "[]uint64", "[]string", "bool", "float64", "int64", "string", "uint64":
			// pass
		default:
			return nil, fmt.Errorf("unsupported data type: %s", typs[i])
		}
	}

	// Ensure max(colIdx) is within size of rr.Columns.
	if len(rrs) > 0 {
		var maxColIdx int
		for i := range idxs {
			if idxs[i] > maxColIdx {
				maxColIdx = idxs[i]
			}
		}
		if maxColIdx >= len(rrs[0].Columns) {
			return nil, fmt.Errorf("column index is out of range: %d", maxColIdx)
		}
	}

	return &RowResponseSorter{
		colIdx:        idxs,
		colDescending: dirs,
		colType:       typs,
		rrs:           rrs,
	}, nil

}

func (r RowResponseSorter) Len() int      { return len(r.rrs) }
func (r RowResponseSorter) Swap(i, j int) { r.rrs[i], r.rrs[j] = r.rrs[j], r.rrs[i] }
func (r RowResponseSorter) Less(i, j int) bool {
	ri := r.rrs[i]
	rj := r.rrs[j]

	for i, idx := range r.colIdx {
		coli := ri.Columns[idx]
		colj := rj.Columns[idx]
		var comp int
		switch r.colType[i] {
		case "[]uint64":
			ai := coli.GetUint64ArrayVal().Vals
			aj := colj.GetUint64ArrayVal().Vals
			comp = func() int {
				for ii := 0; ii < len(ai); ii++ {
					if len(aj) == ii {
						return 1
					}
					piv := ai[ii]
					pjv := aj[ii]
					if piv == pjv {
						continue
					} else if piv < pjv {
						return -1
					} else {
						return 1
					}
				}
				if len(aj) > len(ai) {
					return -1
				}
				return 0
			}()
		case "[]string":
			ai := coli.GetStringArrayVal().Vals
			aj := colj.GetStringArrayVal().Vals
			comp = func() int {
				for ii := 0; ii < len(ai); ii++ {
					if len(aj) == ii {
						return 1
					}
					sComp := strings.Compare(ai[ii], aj[ii])
					if sComp == 0 {
						continue
					} else {
						return sComp
					}
				}
				if len(aj) > len(ai) {
					return -1
				}
				return 0
			}()
		case "bool":
			bi := coli.GetBoolVal()
			bj := colj.GetBoolVal()
			if bi == bj {
				comp = 0
			} else if !bi && bj {
				comp = -1
			} else {
				comp = 1
			}
		case "float64":
			fi := coli.GetFloat64Val()
			fj := colj.GetFloat64Val()
			if fi == fj {
				comp = 0
			} else if fi < fj {
				comp = -1
			} else {
				comp = 1
			}
		case "int64":
			ni := coli.GetInt64Val()
			nj := colj.GetInt64Val()
			if ni == nj {
				comp = 0
			} else if ni < nj {
				comp = -1
			} else {
				comp = 1
			}
		case "string":
			comp = strings.Compare(coli.GetStringVal(), colj.GetStringVal())
		case "uint64":
			ni := coli.GetUint64Val()
			nj := colj.GetUint64Val()
			if ni == nj {
				comp = 0
			} else if ni < nj {
				comp = -1
			} else {
				comp = 1
			}
		}

		isDescending := r.colDescending[i]

		switch comp {
		case 0:
			continue
		case -1:
			if isDescending {
				return false
			}
			return true
		case 1:
			if isDescending {
				return true
			}
			return false
		}
	}
	return false
}