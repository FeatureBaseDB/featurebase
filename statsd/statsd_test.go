// Copyright 2021 Molecula Corp. All rights reserved.
package statsd_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/molecula/featurebase/v3/statsd"
	_ "github.com/molecula/featurebase/v3/test"
)

func TestStatsClient_WithTags(t *testing.T) {
	// Create a new client.
	c, err := statsd.NewStatsClient("localhost:19444", "testnamespace")
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

func TestStatsClient_Methods(t *testing.T) {
	// Create a new client.
	c, err := statsd.NewStatsClient("localhost:19444", "testnamespace")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	dur, _ := time.ParseDuration("123us")
	c.CountWithCustomTags("ct", 1, 1.0, []string{"foo:bar"})
	c.Count("cc", 1, 1.0)
	c.Gauge("gg", 10, 1.0)
	c.Histogram("hh", 1, 1.0)
	c.Timing("tt", dur, 1.0)
	c.Set("ss", "ss", 1.0)
}
