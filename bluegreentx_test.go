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

package pilosa

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	cryrand "crypto/rand"

	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
)

var _ = context.Background
var _ = os.Open
var _ = strings.Split

func TestMultiReaderB(t *testing.T) {
	// MultiReaderB should read identical chunks of bytes from both its "a" and "b"
	// member io.Readers, else it should panic. This should hold for
	// varying sizes of inputs.

	for n := 1 << 5; n < (1 << 18); n = n*2 - 13 {
		src := io.LimitReader(cryrand.Reader, int64(n))

		a := make([]byte, n)
		nr := 0
		for nr < n {
			na, err := src.Read(a)
			PanicOn(err)
			nr += na
		}
		if nr != n {
			panic("short read")
		}

		b := make([]byte, n)
		copy(b, a)
		if !bytes.Equal(a, b) {
			panic("test prep failed")
		}

		m := &MultiReaderB{
			a: ioutil.NopCloser(bytes.NewBuffer(a)),
			b: ioutil.NopCloser(bytes.NewBuffer(b)),
		}

		// should not trigger the internal panic of MultiReadB
		ncp, err := io.Copy(ioutil.Discard, m)
		PanicOn(err)
		if ncp != int64(n) {
			panic("short copy")
		}

		for victim := 0; victim < n; victim += 7 {

			copy(b, a)
			if victim%2 == 0 {
				// corrupt b
				b[victim] = (b[victim] + 1) % 255
			} else {
				// corrupt a
				a[victim] = (a[victim] + 1) % 255
			}
			m = &MultiReaderB{
				a: ioutil.NopCloser(bytes.NewBuffer(a)),
				b: ioutil.NopCloser(bytes.NewBuffer(b)),
			}
			helperShouldPanicOnCopy(m)
		}
	}
}

func helperShouldPanicOnCopy(m *MultiReaderB) {
	// differences in bytes read should be noticed
	defer func() {
		r := recover()
		if r == nil {
			panic("expected panic on byte difference but didn't see it")
		}
	}()
	_, _ = io.Copy(ioutil.Discard, m)
}
