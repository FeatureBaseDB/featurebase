// Copyright 2021 Molecula Corp. All rights reserved.
package http

// Error defines a standard application error.
type Error struct {
	// Human-readable message.
	Message string `json:"message"`
}

// Error returns the string representation of the error message.
func (e *Error) Error() string {
	return e.Message
}
