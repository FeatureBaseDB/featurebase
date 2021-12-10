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

func VersionInfo(rename bool) string {
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
	productName := "Pilosa"
	if rename {
		productName = "FeatureBase"
	}
	return prefix + productName + suffix
}
