// Copyright 2013 Gary Burd. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package websocket

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"testing"
	"testing/iotest"
	"time"
)

var timeoutErrImplementsNetError net.Error = errWriteTimeout

type fakeNetConn struct {
	io.Reader
	io.Writer
}

func (c fakeNetConn) Close() error                       { return nil }
func (c fakeNetConn) LocalAddr() net.Addr                { return nil }
func (c fakeNetConn) RemoteAddr() net.Addr               { return nil }
func (c fakeNetConn) SetDeadline(t time.Time) error      { return nil }
func (c fakeNetConn) SetReadDeadline(t time.Time) error  { return nil }
func (c fakeNetConn) SetWriteDeadline(t time.Time) error { return nil }

func TestFraming(t *testing.T) {
	frameSizes := []int{0, 1, 2, 124, 125, 126, 127, 128, 129, 65534, 65535, 65536, 65537}
	var readChunkers = []struct {
		name string
		f    func(io.Reader) io.Reader
	}{
		{"half", iotest.HalfReader},
		{"one", iotest.OneByteReader},
		{"asis", func(r io.Reader) io.Reader { return r }},
	}

	writeBuf := make([]byte, 65537)
	for i := range writeBuf {
		writeBuf[i] = byte(i)
	}

	for _, isServer := range []bool{true, false} {
		for _, chunker := range readChunkers {

			var connBuf bytes.Buffer
			wc := newConn(fakeNetConn{Reader: nil, Writer: &connBuf}, isServer, 1024, 1024)
			rc := newConn(fakeNetConn{Reader: chunker.f(&connBuf), Writer: nil}, !isServer, 1024, 1024)

			for _, n := range frameSizes {
				for _, iocopy := range []bool{true, false} {
					name := fmt.Sprintf("s:%v, r:%s, n:%d c:%v", isServer, chunker.name, n, iocopy)

					w, err := wc.NextWriter(TextMessage)
					if err != nil {
						t.Errorf("%s: wc.NextWriter() returned %v", name, err)
						continue
					}
					var nn int
					if iocopy {
						var n64 int64
						n64, err = io.Copy(w, bytes.NewReader(writeBuf[:n]))
						nn = int(n64)
					} else {
						nn, err = w.Write(writeBuf[:n])
					}
					if err != nil || nn != n {
						t.Errorf("%s: w.Write(writeBuf[:n]) returned %d, %v", name, nn, err)
						continue
					}
					err = w.Close()
					if err != nil {
						t.Errorf("%s: w.Close() returned %v", name, err)
						continue
					}

					opCode, r, err := rc.NextReader()
					if err != nil || opCode != TextMessage {
						t.Errorf("%s: NextReader() returned %d, r, %v", name, opCode, err)
						continue
					}
					rbuf, err := ioutil.ReadAll(r)
					if err != nil {
						t.Errorf("%s: ReadFull() returned rbuf, %v", name, err)
						continue
					}

					if len(rbuf) != n {
						t.Errorf("%s: len(rbuf) is %d, want %d", name, len(rbuf), n)
						continue
					}

					for i, b := range rbuf {
						if byte(i) != b {
							t.Errorf("%s: bad byte at offset %d", name, i)
							break
						}
					}
				}
			}
		}
	}
}

func TestReadLimit(t *testing.T) {

	const readLimit = 512
	message := make([]byte, readLimit+1)

	var b1, b2 bytes.Buffer
	wc := newConn(fakeNetConn{Reader: nil, Writer: &b1}, false, 1024, readLimit-2)
	rc := newConn(fakeNetConn{Reader: &b1, Writer: &b2}, true, 1024, 1024)
	rc.SetReadLimit(readLimit)

	// Send message at the limit with interleaved pong.
	w, _ := wc.NextWriter(BinaryMessage)
	w.Write(message[:readLimit-1])
	wc.WriteControl(PongMessage, []byte("this is a pong"), time.Now().Add(10*time.Second))
	w.Write(message[:1])
	w.Close()

	// Send message larger than the limit.
	wc.WriteMessage(BinaryMessage, message[:readLimit+1])

	op, _, err := rc.NextReader()
	if op != BinaryMessage || err != nil {
		t.Fatalf("1: NextReader() returned %d, %v", op, err)
	}
	op, r, err := rc.NextReader()
	if op != BinaryMessage || err != nil {
		t.Fatalf("2: NextReader() returned %d, %v", op, err)
	}
	_, err = io.Copy(ioutil.Discard, r)
	if err != ErrReadLimit {
		t.Fatalf("io.Copy() returned %v", err)
	}
}
