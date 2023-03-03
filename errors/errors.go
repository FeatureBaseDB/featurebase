// Package errors wraps pkg/errors and includes some custom features such as
// error codes.
package errors

import (
	"encoding/json"
	"io"

	"github.com/pkg/errors"
)

// Code is an error code which can be used to check against a given error. For
// example, see the Is() method.
type Code string

func New(code Code, message string) error {
	return errors.WithStack(codedError{
		Code:    code,
		Message: message,
	})
}

func As(err error, target interface{}) bool {
	return errors.As(err, target)
}

func Cause(err error) error {
	return errors.Cause(err)
}

func Errorf(format string, args ...interface{}) error {
	return errors.Errorf(format, args...)
}

// Is is a fork of the Is() method from `pkg/errors` which takes as its target
// an error Code instead of an error.
func Is(err error, target Code) bool {
	match := codedError{
		Code: target,
	}
	return errors.Is(err, match)
}

func Unwrap(err error) error {
	return errors.Unwrap(err)
}

func WithMessage(err error, message string) error {
	return errors.WithMessage(err, message)
}

func WithMessagef(err error, format string, args ...interface{}) error {
	return errors.WithMessagef(err, format, args...)
}

func WithStack(err error) error {
	return errors.WithStack(err)
}

func Wrap(err error, message string) error {
	return errors.Wrap(err, message)
}

func Wrapf(err error, fmt string, args ...interface{}) error {
	return errors.Wrapf(err, fmt, args...)
}

// codedError is the fundamental type used by this package to provide coded
// errors.
type codedError struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
	Wrapped string `json:"wrapped,omitempty"`
}

func (ce codedError) Error() string {
	if ce.Wrapped != "" {
		return ce.Wrapped
	}
	return ce.Message
}

// func (ce codedError) As(target interface{}) bool {
// 	return false
// }

func (ce codedError) Is(err error) bool {
	if e, ok := err.(codedError); ok && ce.Code == e.Code {
		return true
	}
	return false
}

const (
	ErrUncoded Code = "Uncoded"
)

// MarshalJSON returns the provided error as a json object (as a string)
// representing a codedError. If err is not already a codedError, the json
// object will still represent a codedError but its `code` value will be empty.
// Note: an empty code here is intentional and is different from code
// `errors.Uncoded` which is a valid code; it just means the developer returned
// a codedError but didn't bother to choose (or create) a useful error code.
func MarshalJSON(err error) string {
	cause := Cause(err)

	var out *codedError

	switch v := cause.(type) {
	case codedError:
		v.Wrapped = err.Error()
		out = &v
	default:
		out = &codedError{
			Message: cause.Error(),
			Wrapped: err.Error(),
		}
	}

	// Marshal the codedError to json as output.
	j, jerr := json.Marshal(out)
	if jerr != nil {
		return out.Error()
	}

	return string(j)

}

// UnmarshalJSON converts the byte slice into a codedError. If the bytes can't
// unmarshal to a codedError, a normal error will be returned containing the
// string value of the byte slice.
func UnmarshalJSON(r io.Reader) error {
	b, _ := io.ReadAll(r)

	out := &codedError{}
	if err := json.Unmarshal(b, out); err != nil {
		return errors.New(string(b))
	}
	return out
}
