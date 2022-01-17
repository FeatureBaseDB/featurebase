// Copyright 2021 Molecula Corp. All rights reserved.
package main

import (
	"os"
	"testing"
)

const testRecords int = 1000

func TestAge(t *testing.T) {
	defer os.Remove("age.csv")
	err := GenerateAgeField(testRecords)
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func TestIP(t *testing.T) {
	defer os.Remove("ip.csv")
	err := GenerateIPField(testRecords)
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func TestIdentifier(t *testing.T) {
	defer os.Remove("identifier.csv")
	err := GenerateArbIdField(testRecords)
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func TestOptIn(t *testing.T) {
	defer os.Remove("optin.csv")
	err := GenerateOptInField(testRecords)
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func TestCountry(t *testing.T) {
	defer os.Remove("country.csv")
	err := GenerateCountryField(testRecords)
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func TestTime(t *testing.T) {
	defer os.Remove("time.csv")
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
