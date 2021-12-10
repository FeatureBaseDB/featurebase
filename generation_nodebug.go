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
