// Copyright 2021 Molecula Corp. All rights reserved.
package main

import (
	"testing"
)

const testRecords int = 1000

func TestAge(t *testing.T) {
	err := GenerateAgeField(testRecords)
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func TestIP(t *testing.T) {
	err := GenerateIPField(testRecords)
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func TestIndentifer(t *testing.T) {
	err := GenerateArbIdField(testRecords)
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func TestOptIn(t *testing.T) {
	err := GenerateOptInField(testRecords)
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func TestCountry(t *testing.T) {
	err := GenerateCountryField(testRecords)
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func TestTime(t *testing.T) {
	err := GenerateTimeField(testRecords)
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func TestGenerateDate(t *testing.T) {
	testDate := generateDate()
	if testDate == "" {
		t.Fatalf("error generating date")
	}
}
