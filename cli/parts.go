package cli

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// query is a collection of queryParts which, when applied together, make up an
// executable SQL query.
type query []queryPart

func (q query) String() string {
	var sb strings.Builder
	for i := range q {
		sb.WriteString(q[i].String())
		if i < len(q)-1 {
			sb.WriteRune('\n')
		}
	}
	return sb.String()
}

// Reader returns the query as an io.Reader so that it can be passed to, for
// example, http.Post().
func (q query) Reader() io.Reader {
	readers := make([]io.Reader, 0, len(q))
	for i := range q {
		readers = append(readers, q[i].Reader())
	}
	return io.MultiReader(readers...)
}

// queryPart is an interface representing anything which can use to build up a
// query.
type queryPart interface {
	fmt.Stringer
	Reader() io.Reader
}

// ////////////////////////////////////////////////////////////////////////////
// raw
// ////////////////////////////////////////////////////////////////////////////

// Ensure type implements interface.
var _ queryPart = (*partRaw)(nil)

type partRaw struct {
	raw string
}

func newPartRaw(s string) *partRaw {
	return &partRaw{
		raw: s,
	}
}

func (p *partRaw) Reader() io.Reader {
	return strings.NewReader(p.raw + "\n")
}

func (p *partRaw) String() string {
	return p.raw
}

// ////////////////////////////////////////////////////////////////////////////
// file
// ////////////////////////////////////////////////////////////////////////////

// Ensure type implements interface.
var _ queryPart = (*partFile)(nil)

type partFile struct {
	file *os.File
}

func newPartFile(f *os.File) *partFile {
	return &partFile{
		file: f,
	}
}

func (p *partFile) Reader() io.Reader {
	return p.file
}

func (p *partFile) String() string {
	return fmt.Sprintf("[file: %s]", p.file.Name())
}

// ////////////////////////////////////////////////////////////////////////////
// batch file
// ////////////////////////////////////////////////////////////////////////////

// Ensure type implements interface.
var _ queryPart = (*partBatchFile)(nil)

type partBatchFile struct {
	file *os.File
}

func (p *partBatchFile) Reader() io.Reader {
	return p.file
}

func (p *partBatchFile) String() string {
	return p.file.Name()
}

// ////////////////////////////////////////////////////////////////////////////
// terminator (i.e. ";")
// ////////////////////////////////////////////////////////////////////////////

// Ensure type implements interface.
var _ queryPart = (*partTerminator)(nil)

type partTerminator struct{}

func newPartTerminator() *partTerminator {
	return &partTerminator{}
}

func (p *partTerminator) Reader() io.Reader {
	return nil
}

func (p *partTerminator) String() string {
	return terminationChar
}
