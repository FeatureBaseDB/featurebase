package pilosa

var Version = "v0.0.0"
var BuildTime = "not recorded"

// ldflags works without this - removing it allows TestHandler_Version to work simply
func SetupVersionBuild() {
	/*
		if Version == "" {
			Version = "v0.0.0"
		}
		if BuildTime == "" {
			BuildTime = "not recorded"
		}
	*/

}
