package cli

import (
	"os"
)

// workingDir was originally set up with the intention of using it to maintain a
// reference to the current working directory. But it turns out we haven't
// really needed that so far. The `cd()` method is unsed in one of the meta
// commands, but we could probably just call `os.Chdir()` directly there. With
// that said, I'm leaving it here for now until we're abosolutely sure we don't
// need to use this for other directory/file handling functionality.
type workingDir struct{}

func newWorkingDir() *workingDir {
	return &workingDir{}
}

func (wd *workingDir) cd(dir string) error {
	return os.Chdir(dir)
}
