// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
