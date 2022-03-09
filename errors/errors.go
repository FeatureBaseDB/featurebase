// Package errors wraps pkg/errors and includes some custom features such as
// error codes.
package errors

import (
	"github.com/pkg/errors"
)

// Code is an error code which can be used to check against a given error. For
// example, see the Is() method.
type Code string

func New(code Code, message string) error {
	return errors.WithStack(codedError{
		code:    code,
		message: message,
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
		code: target,
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
	code    Code
	message string
}

func (ce codedError) Error() string {
	return ce.message
}

// func (ce codedError) As(target interface{}) bool {
// 	return false
// }

func (ce codedError) Is(err error) bool {
	if e, ok := err.(codedError); ok && ce.code == e.code {
		return true
	}
	return false
}

const (
	ErrUncoded Code = "Uncoded"
)
