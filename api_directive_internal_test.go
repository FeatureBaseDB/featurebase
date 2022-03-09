package pilosa

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestThingsAddedGeneric(t *testing.T) {
	from := []string{"a", "b", "c"}
	to := []string{"b", "c", "d"}

	added := thingsAdded(from, to)
	assert.Equal(t, added, []string{"d"})
}

func TestSliceComparer(t *testing.T) {
	from := []string{"a", "b", "c"}
	to := []string{"b", "c", "d"}

	sc := newSliceComparer(from, to)

	added := sc.added()
	assert.Equal(t, added, []string{"d"})
}
