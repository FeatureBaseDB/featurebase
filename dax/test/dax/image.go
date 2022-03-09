package dax

import (
	"strings"
	"testing"

	"github.com/molecula/featurebase/v3/dax/test/docker"
	"github.com/stretchr/testify/require"
)

func imagePull(t *testing.T, imageName ...string) {
	t.Helper()
	for _, img := range imageName {
		// If the images is something other than one at docker.io, then don't
		// perform the `docker pull`. This allows a develper to swap out the
		// ImageName with a local image.
		if !strings.HasPrefix(img, "docker.io/") {
			continue
		}
		if err := docker.ImagePull(img); err != nil {
			require.NoError(t, err)
		}
	}
}
