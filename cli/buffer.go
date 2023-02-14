package cli

import (
	"io"
	"strings"

	"github.com/featurebasedb/featurebase/v3/errors"
)

// buffer is a query buffer for SQL statements. Note that this is not a query
// buffer as you would find on a database server (buffering query results).
// Rather, this buffers the working SQL statement. The buffer has two
// components: the buffer of query parts making up the working, incomplete SQL
// statement, and the last completed SQL statement submitted to the Queryer.
type buffer struct {
	parts     []queryPart
	lastQuery query

	hasBatchFile bool
}

func newBuffer() *buffer {
	return &buffer{}
}

// addPart adds the given queryPart to the buffer. If the part is of type
// `partTerminator` (which is generally singified in the CLI by a ";"), the
// buffer will finalize the query and return it. In all other cases, the
// returned query is nil.
func (b *buffer) addPart(part queryPart) (query, error) {
	// Check for part type compatibility. For example, multiple batchFile parts
	// are not allowed in the same query.
	switch part.(type) {
	case *partBatchFile:
		if b.hasBatchFile {
			return nil, errors.Errorf("multiple batch files in one query is not supported")
		}
		b.hasBatchFile = true
	case *partTerminator:
		return b.finalize(), nil
	}

	b.parts = append(b.parts, part)
	return nil, nil
}

// finalize copies the contents (queryParts) of buffer to lastQuery and then
// resets the buffer. It returns the query that was finalized.
func (b *buffer) finalize() query {
	q := make(query, len(b.parts))
	copy(q, b.parts)
	b.lastQuery = q
	b.reset()
	return q

}

// print returns the contents of the buffer as a string. This is generally used
// to visually inspect the state of the buffer (for example, when a user issues
// a `\p` meta-command in the CLI).
func (b *buffer) print() string {
	if len(b.parts) > 0 {
		return query(b.parts).String()
	} else if b.lastQuery != nil {
		return b.lastQuery.String() + ";"
	}
	return "Query buffer is empty."
}

// reset clears the buffer. It returns a message which may optionally be used to
// display to a user.
func (b *buffer) reset() string {
	b.parts = b.parts[:0]
	b.hasBatchFile = false
	return "Query buffer reset (cleared)."
}

func (b *buffer) Reader() io.Reader {
	if len(b.parts) > 0 {
		return query(b.parts).Reader()
	} else if b.lastQuery != nil {
		r := b.lastQuery.Reader()
		// TODO(tlt): terminating the query here results in a line feed just
		// before the semi-colon (for example, when you print out the query
		// buffer using `\w [FILE]`). The removal and re-introduction of line
		// feeds is kind of a mess.
		term := strings.NewReader(";")
		return io.MultiReader(r, term)
	}
	return strings.NewReader("")
}
