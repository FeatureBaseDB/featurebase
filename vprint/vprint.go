// Copyright 2021 Molecula Corp. All rights reserved.
package vprint

import (
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

const RFC3339MsecTz0 = "2006-01-02T15:04:05.000Z07:00"
const RFC3339UsecTz0 = "2006-01-02T15:04:05.000000Z07:00"

// for tons of debug output
var VerboseVerbose bool = false

func init() {
	// keeper linter happy
	_ = DirExists
	_ = FileExists
	_ = Caller
	_ = Stack
	_ = RFC3339MsecTz0
	_ = RFC3339UsecTz0
	_ = AlwaysPrintf
	_ = FileSize
}

func PanicOn(err interface{}) {
	switch v := err.(type) {
	case string:
		if v != "" {
			panic(v)
		}
	default: // error
		if v != nil {
			panic(v)
		}
	}
}

func PP(format string, a ...interface{}) {
	if VerboseVerbose {
		TSPrintf(format, a...)
	}
}

func VV(format string, a ...interface{}) {
	TSPrintf(format, a...)
}

func AlwaysPrintf(format string, a ...interface{}) {
	TSPrintf(format, a...)
}

var tsPrintfMut sync.Mutex

// time-stamped printf
func TSPrintf(format string, a ...interface{}) {
	tsPrintfMut.Lock()
	_, _ = Printf("\n%s %s ", FileLine(3), ts())
	_, _ = Printf(format+"\n", a...)
	tsPrintfMut.Unlock()
}

// get timestamp for logging purposes
func ts() string {
	return time.Now().Format(RFC3339UsecTz0)
}

// so we can multi write easily, use our own printf
var OurStdout io.Writer = os.Stdout

// Printf formats according to a format specifier and writes to standard output.
// It returns the number of bytes written and any write error encountered.
func Printf(format string, a ...interface{}) (n int, err error) {
	return fmt.Fprintf(OurStdout, format, a...)
}

func FileLine(depth int) string {
	_, fileName, fileLine, ok := runtime.Caller(depth)
	var s string
	if ok {
		s = fmt.Sprintf("%s:%d", path.Base(fileName), fileLine)
	} else {
		s = ""
	}
	return s
}

func Stack() string {
	return string(debug.Stack())
}

func FileExists(name string) bool {
	fi, err := os.Stat(name)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return false
	}
	return true
}

func DirExists(name string) bool {
	fi, err := os.Stat(name)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return true
	}
	return false
}

func FileSize(name string) (int64, error) {
	fi, err := os.Stat(name)
	if err != nil {
		return -1, err
	}
	return fi.Size(), nil
}

// Caller returns the name of the calling function.
func Caller(upStack int) string {
	// elide ourself and runtime.Callers
	target := upStack + 2

	pc := make([]uintptr, target+2)
	n := runtime.Callers(0, pc)

	f := runtime.Frame{Function: "unknown"}
	if n > 0 {
		frames := runtime.CallersFrames(pc[:n])
		for i := 0; i <= target; i++ {
			contender, more := frames.Next()
			if i == target {
				f = contender
			}
			if !more {
				break
			}
		}
	}
	return f.Function
}
