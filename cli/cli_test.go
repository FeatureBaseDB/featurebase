package cli_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/cli"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestCLI(t *testing.T) {
	t.Run("Input", func(t *testing.T) {
		ctx := context.Background()

		capture := newCapture(t)

		cli := cli.NewCommand(logger.StderrLogger)
		cli.Stdin = capture
		cli.Stdout = capture
		cli.Queryer = capture

		go func() {
			assert.NoError(t, cli.Run(ctx))
		}()

		none := []string{}

		// One statement, one line.
		capture.Assert("one;", []string{"one\n"})

		// One statement, multiple lines.
		capture.Assert("one", none)
		capture.Assert(" two ", none)
		capture.Assert("three;", []string{"one\ntwo\nthree\n"})

		// Multiple statements, one line.
		capture.Assert("foo; bar;", []string{"foo\n", "bar\n"})

		// Multiple statements, multiple lines.
		capture.Assert("a1", none)
		capture.Assert("a2; b1", []string{"a1\na2\n"})
		capture.Assert("b2;", []string{"b1\nb2\n"})

		// Blank lines.
		capture.Assert("one", none)
		capture.Assert("", none)
		capture.Assert("three;", []string{"one\nthree\n"})

		// Just a semi-colon.
		capture.Assert(";", []string{""})

		// Multi-line with just a semi-colon.
		capture.Assert("one", none)
		capture.Assert(";", []string{"one\n"})

		// Ensure a clean exit with no errors.
		assert.NoError(t, capture.Exit())
	})
}

////////////////////////////////////////////////////////

// Ensure type implementes interface.
var _ io.ReadCloser = (*capture)(nil)
var _ io.Writer = (*capture)(nil)
var _ cli.Queryer = (*capture)(nil)

// capture implements the various CLI interfaces in order to capture test input
// and submit it as though that input were being read from the command line. It
// also captures calls made to the Queryer.Query method and ensures the sql the
// contain is expected.
type capture struct {
	t *testing.T

	// ch is a channel of strings (one line at a time) of CLI input.
	ch chan string

	mu   sync.RWMutex
	sqls []string

	// queryDone will receive an event any time the Query method is called and
	// has completed. This is to tell the Assert method that it's safe to
	// compare the sqls slice.
	queryDone chan struct{}

	asserting chan struct{}

	err error
}

func newCapture(t *testing.T) *capture {
	return &capture{
		t:         t,
		ch:        make(chan string),
		sqls:      make([]string, 0),
		queryDone: make(chan struct{}),
	}
}

func (c *capture) Exit() error {
	c.sendLine(`\q`)
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.err
}

func (c *capture) Assert(in string, out []string) {
	c.asserting = make(chan struct{})

	c.sendLine(in)

	// Wait for the CLI command to complete processing the input and send the
	// sql to Query() by blocking on the queryDone channel. Because Query gets
	// called for every sql statement in the input, an input resulting in
	// multiple sql statements needs to wait for all expected queries to
	// complete. A timeout is included to this so it doesn't deadlock in the
	// case where Query is expected to be called, but isn't; after the timeout,
	// the test should fail completely. In summary: we wait on queryDone the
	// number of sql statements we expect. If we receive fewer than expected,
	// the timeout will occur. If we receive more than expected, the Query()
	// method will effectively deadlock, reach its own timout, then write to
	// capture.err, which will be reported upon Exit().
	for range out {
		select {
		case <-c.queryDone:
		case <-time.After(2 * time.Second):
			c.t.Fatalf("expected Query() to be called")
		}
	}

	close(c.asserting)

	c.mu.Lock()
	defer c.mu.Unlock()

	assert.Equal(c.t, out, c.sqls)

	// Reset the slice.
	c.sqls = c.sqls[:0]

}

// sendLine sends the given string as a line input to the CLI command. It
// appends a line feed to the end of string in order to mimic the user hitting
// the return key.
func (c *capture) sendLine(s string) {
	// Add a line feed before putting s on the channel in order to mimic the
	// user hitting the return key.
	c.ch <- s + "\n"
}

// Read is read by the CLI in place of user input. It effectively sends lines of
// input to the CLI, getting each line to be sent off the channel.
func (c *capture) Read(b []byte) (n int, err error) {
	s := <-c.ch
	return strings.NewReader(s).Read(b)
}

func (c *capture) Close() error {
	close(c.ch)
	return nil
}

// Write is called with anything written to output. This would included results
// from calling Query() under normal, non-testing conditions, as well as other
// informational text sent to output, such as the splash message.
func (c *capture) Write(b []byte) (n int, err error) {
	return 0, nil
}

// Query is called by the CLI command once a full SQL statement is received
// (signified by the terminator: `;`).
func (c *capture) Query(org string, db string, sql io.Reader) (*featurebase.WireQueryResponse, error) {
	tmpBuf := new(strings.Builder)
	_, err := io.Copy(tmpBuf, sql)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.sqls = append(c.sqls, tmpBuf.String())
	c.mu.Unlock()

	select {
	case c.queryDone <- struct{}{}:
	case <-c.asserting:
		c.mu.Lock()
		c.err = errors.Errorf("unexpected query: %s", sql)
		c.mu.Unlock()
	}
	return &featurebase.WireQueryResponse{}, nil
}
