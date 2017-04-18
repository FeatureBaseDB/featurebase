package pilosa

import "io"

// CmdIO holds standard unix inputs and outputs.
type CmdIO struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewCmdIO returns a new instance of CmdIO with inputs and outputs set to the
// arguments.
func NewCmdIO(stdin io.Reader, stdout, stderr io.Writer) *CmdIO {
	return &CmdIO{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}
