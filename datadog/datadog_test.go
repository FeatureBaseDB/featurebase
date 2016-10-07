package datadog_test

import (
	"reflect"
	"testing"

	"github.com/umbel/pilosa/datadog"
)

func TestStatsClient_WithTags(t *testing.T) {
	// Create a new client.
	c, err := datadog.NewStatsClient()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Create a new client with additional tags.
	c1 := c.WithTags("foo", "bar")
	if tags := c1.Tags(); !reflect.DeepEqual(tags, []string{"bar", "foo"}) {
		t.Fatalf("unexpected tags: %+v", tags)
	}

	// Create a new client from the clone with more tags.
	c2 := c1.WithTags("bar", "baz")
	if tags := c2.Tags(); !reflect.DeepEqual(tags, []string{"bar", "baz", "foo"}) {
		t.Fatalf("unexpected tags: %+v", tags)
	}
}
