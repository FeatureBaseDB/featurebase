// Copyright 2021 Molecula Corp. All rights reserved.
package proto

import (
	"reflect"
	"testing"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/pb"
)

func testOneRoundTrip(t *testing.T, s pilosa.Serializer, obj pilosa.Message, expectedMarshalErr error, expectedUnmarshalErr error, expectedMismatchErr error) {
	repr, err := s.Marshal(obj)
	if err != nil {
		if expectedMarshalErr == nil {
			t.Fatalf("unexpected marshalling error %q", err.Error())
		}
		if err.Error() != expectedMarshalErr.Error() {
			t.Fatalf("expecting marshalling error %q, got %q", expectedMarshalErr.Error(), err.Error())
		}
	} else {
		if expectedMarshalErr != nil {
			t.Fatalf("expected marshalling error %q, got no error", expectedMarshalErr.Error())
		}
	}

	obj2 := reflect.New(reflect.TypeOf(obj).Elem()).Interface()
	err = s.Unmarshal(repr, obj2)
	if err != nil {
		if expectedUnmarshalErr == nil {
			t.Fatalf("unexpected unmarshalling error %q", err.Error())
		}
		if err.Error() != expectedUnmarshalErr.Error() {
			t.Fatalf("expecting unmarshalling error %q, got %q", expectedUnmarshalErr.Error(), err.Error())
		}
	} else {
		if expectedUnmarshalErr != nil {
			t.Fatalf("expected unmarshalling error %q, got no error", expectedUnmarshalErr.Error())
		}
	}
	if !reflect.DeepEqual(obj, obj2) {
		t.Fatalf("serialization round trip failed for %T:\nexpected %#v\ngot %#v", obj, obj, obj2)
	}
}

func TestEncodeDecodeDistinctTimestamp(t *testing.T) {
	s := Serializer{}
	pbTime := pb.DistinctTimestamp{
		Values: []string{"this", "is", "fake", "timestamp", "values"},
		Name:   "pbtime",
	}
	piloTime := pilosa.DistinctTimestamp{
		Values: []string{"this", "is", "fake", "timestamp", "values"},
		Name:   "pbtime",
	}
	decoded := s.decodeDistinctTimestamp(&pbTime)
	if !reflect.DeepEqual(decoded, piloTime) {
		t.Errorf("failed to decode DistinctTimestamp. expected %v got %v", piloTime, decoded)
	}
	encoded := s.encodeDistinctTimestamp(piloTime)
	if !reflect.DeepEqual(encoded, &pbTime) {
		t.Errorf("failed to encode DistinctTimestamp. expected %v got %v", &pbTime, encoded)
	}
}

func TestDecodeQueryResult(t *testing.T) {
	t.Run("DistinctTimestamp", func(t *testing.T) {
		pbTime := pb.DistinctTimestamp{
			Values: []string{"this", "is", "fake", "timestamp", "values"},
			Name:   "pbtime",
		}
		piloTime := pilosa.DistinctTimestamp{
			Values: []string{"this", "is", "fake", "timestamp", "values"},
			Name:   "pbtime",
		}
		q := &pb.QueryResult{Type: queryResultTypeDistinctTimestamp, DistinctTimestamp: &pbTime}
		s := Serializer{}
		decoded := s.decodeQueryResult(q)
		if !reflect.DeepEqual(decoded, piloTime) {
			t.Errorf("failed to decode DistinctTimestamp. expected %v got %v", piloTime, decoded)
		}
	})
}
