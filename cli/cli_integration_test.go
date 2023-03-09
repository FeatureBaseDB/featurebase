package cli_test

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/featurebasedb/featurebase/v3/cli"
	"github.com/featurebasedb/featurebase/v3/dax/server/test"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/stretchr/testify/require"
)

func TestCLIIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	t.Run("Stubbed Framework", func(t *testing.T) {
		mc := test.MustRunManagedCommand(t)
		defer mc.Close()

		addr := mc.Address()

		capture := newCapture(t)

		comparer := newComparer(t)
		comparer.run()

		fbsql := cli.NewCommand(logger.StderrLogger)
		fbsql.Stdin = capture
		fbsql.SetStdout(comparer)
		fbsql.Stderr = comparer

		fbsql.Config = &cli.Config{
			Host: addr.Host(),
			Port: fmt.Sprintf("%d", addr.Port()),
		}

		// Run fbsql in a goroutine so we can continue to send it commands
		// below.
		didQuit := make(chan struct{})
		go func() {
			require.NoError(t, fbsql.Run(ctx))
			close(didQuit)
		}()

		// testFiles reference files located in the cli/testdata directory. All
		// tests should be placed there; other than adding another test file to
		// this list, you probably shouldn't be editing this file unless you are
		// trying to modify the way the test framework itself works.
		testFiles := []string{
			"setup",
			"database",
			"table",
			// the tests below may be dependent on the previous tests, which do
			// setup and some shared database and table creation.
			"query_buffer",
			// meta commands
			"meta_bang",
			"meta_cd",
			"meta_echo",
			"meta_file",
			"meta_pset_border",
			"meta_pset_expanded",
			"meta_pset_tuples_only",
			"meta_include",
			"meta_output",
			"meta_set",
			"meta_timing",
			"meta_write",
		}

		for _, testFile := range testFiles {
			t.Run(testFile, func(t *testing.T) {
				f, err := os.Open("testdata/" + testFile)
				require.NoError(t, err)

				scanner := bufio.NewScanner(f)
				var lineNo int
				for scanner.Scan() {
					line := scanner.Text()
					lineNo++

					// Empty lines and comments (//) are ignored.
					if line == "" {
						continue
					} else if strings.HasPrefix(line, "//") {
						continue
					}

					parts := strings.SplitN(line, ":", 2)

					switch parts[0] {
					case "SEND":
						v := ""
						if len(parts) == 2 {
							v = parts[1]
						}
						capture.sendLine(v)
					case "EXPECT":
						v := ""
						if len(parts) == 2 {
							v = parts[1]
						}
						comparer.expectLine(v, testFile, lineNo)
					case "EXPECTCOMP":
						if len(parts) == 2 {
							comps := strings.SplitN(parts[1], ":", 2)
							v := ""
							if len(comps) == 2 {
								v = comps[1]
							}
							comparer.expectLineComp(comparator(comps[0]), v, testFile, lineNo)
						} else {
							t.Errorf("unexpected line: %s[%d]:%s", testFile, lineNo, line)
						}
					default:
						t.Errorf("unexpected line: %s[%d]:%s", testFile, lineNo, line)
					}
				}
				require.NoError(t, scanner.Err())
			})
		}

		// End with quit to ensure that fbsql closes without error.
		capture.sendLine(`\q`)

		// Ensure fbsql quits cleanly.
		select {
		case <-didQuit:
		case <-time.After(time.Second):
			t.Fatalf("expected fbsql to quit")
		}
	})
}

// compare is used to compare fbsql output written to its Stdout with expected
// lines.
type comparer struct {
	t       *testing.T
	out     chan byte
	outline chan []byte
	exp     chan []byte
}

func newComparer(t *testing.T) *comparer {
	return &comparer{
		t:       t,
		out:     make(chan byte, 1024),
		outline: make(chan []byte, 128),
		exp:     make(chan []byte, 1024),
	}
}

func (c *comparer) run() {
	// Read bytes off output, and for every line (designated by a line feed "\n"),
	// push the line onto the outline channel.
	go func() {
		var line []byte
		for {
			b := <-c.out
			if b == byte('\n') {
				c.outline <- line
				line = []byte{}
				continue
			}
			line = append(line, b)
		}
	}()
}

type comparator string

const (
	compEquals     = "Equals"
	compHasPrefix  = "HasPrefix"
	compWithFormat = "WithFormat"
)

// expectLine is a convenience method which calls expectLineComp with the compEq
// comparator and the given line.
func (c *comparer) expectLine(line string, fileName string, lineNo int) {
	c.expectLineComp(compEquals, line, fileName, lineNo)
}

// expectLineComp reads the next line from the outline channel and compares it
// with the given `line`. A comparator can be provided to inform how the lines
// should be compared (for example, the compHasPrefix comparator will just
// compare the beginning part of the outline).
func (c *comparer) expectLineComp(comp comparator, line string, fileName string, lineNo int) {
	var outline []byte
	select {
	case outline = <-c.outline:
	case <-time.After(10 * time.Second):
		// TODO(tlt): this is 10 seconds to account for the fb_views creation on
		// a local mac. This should really be something like 2 seconds. Put this
		// back to 2 once fb_views issue is addressed.
		c.t.Fatalf("expected output line %s[%d]: >%s<", fileName, lineNo, line)
	}

	// msg is included in any require which fails.
	msg := []interface{}{"exp: %s[%d], got: >%s<", fileName, lineNo, outline}

	switch comp {
	case compEquals:
		require.Equal(c.t, []byte(line), outline, msg...)
	case compHasPrefix:
		require.True(c.t, strings.HasPrefix(string(outline), line), msg...)
	case compWithFormat:
		require.True(c.t, compareByteSlices(outline, []byte(line)), msg...)
	default:
		c.t.Fatalf("invalid comparator: %s", comp)
	}
}

func (c *comparer) Write(b []byte) (n int, err error) {
	for i := range b {
		c.out <- b[i]
	}
	return len(b), err
}

// compareByteSlices compares a byte slice s with another byte slice format and
// returns true if they are the same. It will accept underscore as a
// single-character wildcard anywhere in slice format.
func compareByteSlices(s, format []byte) bool {
	// Replace some helpers in format before comparing.
	f := string(format)
	f = strings.ReplaceAll(f, `{uuid}`, `________-____-____-____-____________`)
	f = strings.ReplaceAll(f, `{timestamp}`, `____-__-__T__:__:__Z`)
	format = []byte(f)

	if len(s) != len(format) {
		return false
	}

	for i := range s {
		if format[i] == '_' {
			continue
		}
		if s[i] != format[i] {
			// log.Printf("DEBUG: characters differ: (%d): '%v' != '%v'", i, s[i], format[i])
			return false
		}
	}

	return true
}
