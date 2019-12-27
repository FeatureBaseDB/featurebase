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
	"fmt"

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
