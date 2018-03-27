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

package test

import (
	"bytes"
	"fmt"
	"io/ioutil"
)

// BufferLogger represents a test Logger that holds log messages
// in a buffer for review.
type BufferLogger struct {
	buf *bytes.Buffer
}

// NewBufferLogger returns a new instance of BufferLogger.
func NewBufferLogger() *BufferLogger {
	return &BufferLogger{
		buf: &bytes.Buffer{},
	}
}

func (b *BufferLogger) Printf(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	_, err := b.buf.WriteString(s)
	if err != nil {
		panic(err)
	}
}

func (b *BufferLogger) Debugf(format string, v ...interface{}) {}

func (b *BufferLogger) ReadAll() ([]byte, error) {
	return ioutil.ReadAll(b.buf)
}
