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
	"io"

	"github.com/molecula/featurebase/v2/logger"
)

// CmdIO holds standard unix inputs and outputs.
type CmdIO struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	logger logger.Logger
}

// NewCmdIO returns a new instance of CmdIO with inputs and outputs set to the
// arguments.
func NewCmdIO(stdin io.Reader, stdout, stderr io.Writer) *CmdIO {
	return &CmdIO{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
		logger: logger.NewStandardLogger(stderr),
	}
}

func (c *CmdIO) Logger() logger.Logger {
	return c.logger
}
