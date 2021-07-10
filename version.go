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

package pilosa

import (
	"runtime"
	"time"
)

var Version string
var Commit string
var Variant string
var BuildTime string
var GoVersion string = runtime.Version()
var TrialDeadline string

func VersionInfo() string {
	var prefix string
	if Variant != "" {
		prefix = Variant + " "
	}
	var suffix string
	if Version != "" {
		suffix = " " + Version
	} else {
		suffix = " v2.x"
	}
	buildTime := BuildTime
	if buildTime != "" {
		// Normalize the build time into a friendly format in the user's time zone.
		if t, err := time.Parse("2006-01-02T15:04:05+0000", BuildTime); err == nil {
			buildTime = t.Local().Format("Jan _2 2006 3:04PM")
		}
	}
	switch {
	case Commit != "" && buildTime != "":
		suffix += " (" + buildTime + ", " + Commit + ")"
	case Commit != "":
		suffix += " (" + Commit + ")"
	case buildTime != "":
		suffix += " (" + buildTime + ")"
	}
	suffix += " " + GoVersion
	if TrialDeadline != "" {
		suffix += " limited time trial ends on: " + TrialDeadline
	}
	return prefix + "FeatureBase" + suffix
}
