package datagen

import (
	"fmt"
	"io"

	"github.com/molecula/featurebase/v3/idk"
)

// Ensure Example implements interface.
var _ Sourcer = (*Example)(nil)

// Example implements Sourcer, and returns a very basic
// data set. It can be used as an example for writing
// additional custom Sourcers.
type Example struct{}

// NewExample returns a new instance of Example.
func NewExample(cfg SourceGeneratorConfig) Sourcer {
	return &Example{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
// NOTE: avro Name fields can only contain certain
// characters.
// See: https://avro.apache.org/docs/current/spec.html#names
func (e *Example) Source(cfg SourceConfig) idk.Source {
	src := &ExampleSource{
		cur:   cfg.startFrom,
		endAt: cfg.endAt,
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},         // 0
			idk.StringField{NameVal: "string"}, // 1
		},
	}

	src.record = make([]interface{}, len(src.schema))
	src.record[0] = uint64(0)
	src.record[1] = make([]byte, 12)

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (e *Example) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (e *Example) DefaultEndAt() uint64 {
	return 100
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (e *Example) Info() string {
	return "generate data for a couple of fields to use as an example"
}

// Ensure ExampleSource implements interface.
var _ idk.Source = (*ExampleSource)(nil)

// ExampleSource is an instance of a source generated
// by the Sourcer implementation Example.
type ExampleSource struct {
	cur   uint64
	endAt uint64

	schema []idk.Field
	record record
}

// Record implements idk.Source.
func (e *ExampleSource) Record() (idk.Record, error) {
	if e.cur >= e.endAt {
		return nil, io.EOF
	}

	// Increment the ID.
	e.record[0] = e.cur
	e.record[1] = []byte(fmt.Sprintf("foo-%d", e.cur))
	e.cur++

	return e.record, nil
}

// Schema implements idk.Source.
func (e *ExampleSource) Schema() []idk.Field {
	return e.schema
}

func (e *ExampleSource) Close() error {
	return nil
}
