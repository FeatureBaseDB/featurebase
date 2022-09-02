// This file is a modified redistribution of reopen (github.com/client9/reopen),
// which is governed by the following license notice:
//
// The MIT License (MIT)
//
// Copyright (c) 2015 Nick Galbreath
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package logger

import (
	"os"
	"sync"
)

// FileWriter that can also be reopened
type FileWriter struct {
	mu   sync.Mutex // ensures close / reopen / write are not called at the same time, protects f
	f    *os.File
	mode os.FileMode
	name string
}

// Close calls the underlyding File.Close()
func (f *FileWriter) Close() error {
	f.mu.Lock()
	err := f.f.Close()
	f.mu.Unlock()
	return err
}

// mutex free version
func (f *FileWriter) reopen() error {
	if f.f != nil {
		f.f.Close()
		f.f = nil
	}
	newf, err := os.OpenFile(f.name, os.O_WRONLY|os.O_APPEND|os.O_CREATE, f.mode)
	if err != nil {
		f.f = nil
		return err
	}
	f.f = newf

	return nil
}

// Reopen the file
func (f *FileWriter) Reopen() error {
	f.mu.Lock()
	err := f.reopen()
	f.mu.Unlock()
	return err
}

// Write implements the stander io.Writer interface
func (f *FileWriter) Write(p []byte) (int, error) {
	f.mu.Lock()
	n, err := f.f.Write(p)
	f.mu.Unlock()
	return n, err
}

// Fd returns the file descriptor of the underlying file.
func (f *FileWriter) Fd() uintptr {
	f.mu.Lock()
	n := f.f.Fd()
	f.mu.Unlock()
	return n
}

// NewFileWriter opens a file for appending and writing and can be reopened.
// it is a ReopenWriteCloser...
func NewFileWriter(name string) (*FileWriter, error) {
	// Standard default mode
	return NewFileWriterMode(name, 0600)
}

// NewFileWriterMode opens a Reopener file with a specific permission
func NewFileWriterMode(name string, mode os.FileMode) (*FileWriter, error) {
	writer := FileWriter{
		f:    nil,
		name: name,
		mode: mode,
	}
	err := writer.reopen()
	if err != nil {
		return nil, err
	}
	return &writer, nil
}
