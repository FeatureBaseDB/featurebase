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
	"io/ioutil"
	"os"
	"testing"

	"github.com/molecula/featurebase/v2/testhook"
)

// TestReopenAppend -- make sure we always append to an existing file
//
// 1. Create a sample file using normal means
// 2. Open a ioreopen.File
//    write line 1
// 3. call Reopen
//    write line 2
// 4. close file
// 5. read file, make sure it contains line0,line1,line2
//
func TestReopenAppend(t *testing.T) {
	forig, err := testhook.TempFile(t, "logger-reopen")
	if err != nil {
		t.Fatalf("unable to create initial file: %v", err)
	}
	fname := forig.Name()

	_, err = forig.Write([]byte("line0\n"))
	if err != nil {
		t.Fatalf("Unable to write initial line %s: %s", fname, err)
	}
	err = forig.Close()
	if err != nil {
		t.Fatalf("Unable to close initial file: %s", err)
	}

	// Test that making a new File appends
	f, err := NewFileWriter(fname)
	if err != nil {
		t.Fatalf("Unable to create %s", fname)
	}
	_, err = f.Write([]byte("line1\n"))
	if err != nil {
		t.Errorf("Got write error1: %s", err)
	}

	// Test that reopen always appends
	err = f.Reopen()
	if err != nil {
		t.Errorf("Got reopen error %s: %s", fname, err)
	}
	_, err = f.Write([]byte("line2\n"))
	if err != nil {
		t.Errorf("Got write error2 on %s: %s", fname, err)
	}
	err = f.Close()
	if err != nil {
		t.Errorf("Got closing error for %s: %s", fname, err)
	}

	out, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Fatalf("Unable read in final file %s: %s", fname, err)
	}

	outstr := string(out)
	if outstr != "line0\nline1\nline2\n" {
		t.Errorf("Result was %s", outstr)
	}
}

// Test that reopen works when Inode is swapped out. That is to say,
// if the previous file has been renamed, we want to get a new file
// that isn't the old one, not keep writing to the old file.
//
// 1. Create a sample file using normal means
// 2. Write to it.
// 3. Rename it.
// 4. Call reopen.
// 5. Write line 2.
// 6. Read file, expecting to see only line 2.
func TestChangeInode(t *testing.T) {
	// Step 1 -- Create a empty sample file
	forig, err := testhook.TempFile(t, "changeInode")
	if err != nil {
		t.Fatalf("Unable to create initial file: %s", err)
	}
	fname := forig.Name()
	err = forig.Close()
	if err != nil {
		t.Fatalf("Unable to close initial file: %s", err)
	}

	// Test that making a new File appends
	f, err := NewFileWriter(fname)
	if err != nil {
		t.Fatalf("Unable to create %s", fname)
	}
	_, err = f.Write([]byte("line1\n"))
	if err != nil {
		t.Errorf("Got write error1: %s", err)
	}

	// Now move file
	err = os.Rename(fname, fname+".orig")
	if err != nil {
		t.Errorf("Renaming error: %s", err)
	}
	// remove the scratch file
	defer os.Remove(fname + ".orig")
	_, err = f.Write([]byte("after1\n"))
	if err != nil {
		t.Errorf("Write error: %s", err)
	}

	// Test that reopen always appends
	err = f.Reopen()
	if err != nil {
		t.Errorf("Got reopen error %s: %s", fname, err)
	}
	_, err = f.Write([]byte("line2\n"))
	if err != nil {
		t.Errorf("Got write error2 on %s: %s", fname, err)
	}
	err = f.Close()
	if err != nil {
		t.Errorf("Got closing error for %s: %s", fname, err)
	}

	out, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Fatalf("Unable read in final file %s: %s", fname, err)
	}
	outstr := string(out)
	if outstr != "line2\n" {
		t.Errorf("Result was %s", outstr)
	}
}
