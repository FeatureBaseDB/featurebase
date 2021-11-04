// Copyright 2019 Pilosa Corp.
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

//go:build !generationdebug
// +build !generationdebug

package pilosa

const generationDebug = false

func registerGeneration(id string) string {
	return id
}

func endGeneration(id string) {
}

func cancelGeneration(id string) {
}

func finalizeGeneration(id string) {
}

//lint:ignore U1000 this is conditional on a build flag, see generation_test.go.
func reportGenerations() []string { //nolint:unused,deadcode
	return nil
}
