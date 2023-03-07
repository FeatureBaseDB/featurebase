package cli_test

import (
	"context"
	"fmt"
	"log"
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

		// convenence function aliases
		sendLine := capture.sendLine
		expectLine := comparer.expectLine
		expectLineComp := comparer.expectLineComp

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

		////////////////////////////////////////////////////////////
		// input command
		////////////////////////////////////////////////////////////

		// Startup splash.
		expectLine(`FeatureBase CLI ()`)
		expectLine(`Type "\q" to quit.`)
		expectLine(`Detected on-prem, serverless deployment.`)
		expectLineComp(compHasPrefix, `Host: http://localhost:`)
		expectLine(`You are not connected to a database.`)

		// Simple \echo.
		sendLine(`\echo foo bar`)
		expectLine(`foo bar`)

		// Show databases.
		sendLine(`SHOW DATABASES;`)
		expectLine(`Organization required. Use \org to set an organization.`)

		// Set org.
		sendLine(`\org acme`)
		expectLine(`You have set organization "acme".`)

		// Show databases now that we have set org.
		sendLine(`SHOW DATABASES;`)
		expectLine(` _id | name | owner | updated_by | created_at | updated_at | units | description `)
		expectLine(`-----+------+-------+------------+------------+------------+-------+-------------`)

		sendLine(`CREATE DATABASE db1 WITH UNITS 1;`)
		expectLine(``)
		expectLine(``)
		sendLine(`SHOW DATABASES;`)
		expectLine(` _id                                  | name | owner | updated_by | created_at                | updated_at                | units | description `)
		expectLine(`--------------------------------------+------+-------+------------+---------------------------+---------------------------+-------+-------------`)
		//expectLineComp(compWithFormat, ` ________-____-____-____-____________ | db1  |       |            | 20__-__-__T__:__:__-__:00 | ____-__-__T__:__:__-__:__ |     1 |             `)
		expectLineComp(compWithFormat, ` {uuid} | db1  |       |            | {timestamp} | {timestamp} |     1 |             `)
		expectLine(``)

		// Check database connection.
		sendLine(`\c`)
		expectLine(`You are not connected to a database.`)

		// Try connecting to an invalid database.
		sendLine(`\c invalid`)
		expectLine(`executing meta command: invalid database: invalid`)

		// Connect to a database.
		sendLine(`\c db1`)
		expectLineComp(compWithFormat, `You are now connected to database "db1" ({uuid}).`)

		// Show tables for database.
		sendLine(`SHOW TABLES;`)
		expectLine(` _id                     | name                    | owner | updated_by | created_at                | updated_at                | keys  | space_used | description `)
		expectLine(`-------------------------+-------------------------+-------+------------+---------------------------+---------------------------+-------+------------+-------------`)
		// TODO(tlt): the system tables are completely masked off because their
		// order is not guaranteed. When order is guaranteed, update these to
		// include the actual system tables names.
		expectLineComp(compWithFormat, ` fb_____________________ | fb_____________________ |       |            | {timestamp} | {timestamp} | false |          0 |             `)
		expectLineComp(compWithFormat, ` fb_____________________ | fb_____________________ |       |            | {timestamp} | {timestamp} | false |          0 |             `)
		expectLineComp(compWithFormat, ` fb_____________________ | fb_____________________ |       |            | {timestamp} | {timestamp} | false |          0 |             `)
		expectLineComp(compWithFormat, ` fb_____________________ | fb_____________________ |       |            | {timestamp} | {timestamp} | false |          0 |             `)
		expectLineComp(compWithFormat, ` fb_____________________ | fb_____________________ |       |            | {timestamp} | {timestamp} | false |          0 |             `)
		expectLine(``)

		// expectLine(`STOPPER`)

		////////////////////////////////////////////////////////////

		// End with quit to ensure that fbsql closes without error.
		sendLine(`\q`)

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
	// push the line onto the ouline channel.
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

type comparitor string

const (
	compEq         = "eq"
	compHasPrefix  = "hasPrefix"
	compWithFormat = "withFormat"
)

// expectLine is a convenience method which calls expectLineComp with the compEq
// comparitor and the given line.
func (c *comparer) expectLine(line string) {
	c.expectLineComp(compEq, line)
}

// expectLineComp reads the next line from the outline channel and compares it
// with the given `line`. A comparitor can be provided to inform how the lines
// should be compared (for example, the compHasPrefix comparitor will just
// compare the beginning part of the outline).
func (c *comparer) expectLineComp(comp comparitor, line string) {
	var outline []byte
	select {
	case outline = <-c.outline:
	case <-time.After(2 * time.Second):
		c.t.Fatalf("expected output line: %s", line)
	}

	// msg is included in any require which fails.
	msg := []interface{}{"GOT: >%s<", outline}

	switch comp {
	case compEq:
		require.Equal(c.t, []byte(line), outline, msg...)
	case compHasPrefix:
		require.True(c.t, strings.HasPrefix(string(outline), line), msg...)
	case compWithFormat:
		require.True(c.t, compareByteSlices(outline, []byte(line)), msg...)
	default:
		c.t.Fatalf("invalid comparitor: %s", comp)
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
	f = strings.ReplaceAll(f, `{timestamp}`, `____-__-__T__:__:__-__:__`)
	format = []byte(f)

	if len(s) != len(format) {
		return false
	}

	for i := range s {
		if format[i] == '_' {
			continue
		}
		if s[i] != format[i] {
			log.Printf("DEEBUG: characters differ: (%d): '%v' != '%v'", i, s[i], format[i])
			return false
		}
	}

	return true
}
