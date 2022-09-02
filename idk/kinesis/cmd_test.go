package kinesis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMain(t *testing.T) {
	m := NewMain()
	assert.NotNil(t, m)
}
