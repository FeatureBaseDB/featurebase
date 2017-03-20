package pilosa

import "io"

type CmdIO struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

func NewCmdIO(stdin io.Reader, stdout, stderr io.Writer) *CmdIO {
	return &CmdIO{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}
