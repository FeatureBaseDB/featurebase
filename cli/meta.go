package cli

import (
	"os"
	"os/exec"
	"strings"
	"unicode"

	"github.com/featurebasedb/featurebase/v3/errors"
)

// action is used to indicate how CLICommand should respond after execution a
// given metaCommand. For example, an action of type "reset" tells CLICommand
// that the buffer has been reset and it needs to change its user prompt.
type action string

const (
	actionNone  = ""
	actionQuit  = "quit"
	actionReset = "reset"
)

// metaCommand is the interface for any type responding to a "\" meta-command.
type metaCommand interface {
	execute(cmd *CLICommand) (action, error)
}

// Ensure type implements interface.
var _ metaCommand = (*metaBang)(nil)
var _ metaCommand = (*metaChangeDirectory)(nil)
var _ metaCommand = (*metaConnect)(nil)
var _ metaCommand = (*metaInclude)(nil)
var _ metaCommand = (*metaPrint)(nil)
var _ metaCommand = (*metaQuit)(nil)
var _ metaCommand = (*metaReset)(nil)
var _ metaCommand = (*metaSet)(nil)

// ////////////////////////////////////////////////////////////////////////////
// bang (!)
// ////////////////////////////////////////////////////////////////////////////
type metaBang struct {
	args []string
}

func newMetaBang(args []string) *metaBang {
	return &metaBang{
		args: args,
	}
}

func (m *metaBang) execute(cmd *CLICommand) (action, error) {
	if len(m.args) == 0 {
		return actionNone, errors.Errorf("meta command '!' requires at least one argument")
	}
	c := exec.Command(m.args[0])
	c.Args = m.args
	c.Stdout = cmd.Stdout
	err := c.Run()
	return actionNone, errors.Wrap(err, "running bang command")
}

// ////////////////////////////////////////////////////////////////////////////
// cd
// ////////////////////////////////////////////////////////////////////////////
type metaChangeDirectory struct {
	args []string
}

func newMetaChangeDirectory(args []string) *metaChangeDirectory {
	return &metaChangeDirectory{
		args: args,
	}
}

func (m *metaChangeDirectory) execute(cmd *CLICommand) (action, error) {
	if len(m.args) != 1 {
		return actionNone, errors.Errorf("meta command 'cd' requires exactly one argument")
	}
	err := cmd.workingDir.cd(m.args[0])
	return actionNone, errors.Wrap(err, "running cd command")
}

// ////////////////////////////////////////////////////////////////////////////
// connect (or c)
// ////////////////////////////////////////////////////////////////////////////
type metaConnect struct {
	args []string
}

func newMetaConnect(args []string) *metaConnect {
	return &metaConnect{
		args: args,
	}
}

func (m *metaConnect) execute(cmd *CLICommand) (action, error) {
	switch len(m.args) {
	case 0:
		if cmd.DatabaseID == "" {
			cmd.Printf("You are not connected to a database.\n")
		} else {
			cmd.Printf("You are now connected to database \"%s\" as user \"???\".\n", cmd.DatabaseID)
		}
		return actionNone, nil
	case 1:
		cmd.DatabaseID = m.args[0]
		return actionReset, nil
	default:
		return actionNone, errors.Errorf("meta command 'connect' takes zero or one argument")
	}
}

// ////////////////////////////////////////////////////////////////////////////
// include (or i)
// ////////////////////////////////////////////////////////////////////////////
type metaInclude struct {
	args []string
}

func newMetaInclude(args []string) *metaInclude {
	return &metaInclude{
		args: args,
	}
}

func (m *metaInclude) execute(cmd *CLICommand) (action, error) {
	if len(m.args) != 1 {
		return actionNone, errors.Errorf("meta command 'include' requires exactly one argument")
	}

	file, err := os.Open(m.args[0])
	if err != nil {
		return actionNone, errors.Wrapf(err, "opening file: %s", m.args[0])
	}

	pf := newPartFile(file)

	// TODO: addPart returns a query. We'll have to revisit this when we update
	// the \i command to accept files containing SQL.
	_, err = cmd.buffer.addPart(pf)
	return actionNone, errors.Wrap(err, "adding part file")
}

// ////////////////////////////////////////////////////////////////////////////
// print (or p)
// ////////////////////////////////////////////////////////////////////////////
type metaPrint struct{}

func newMetaPrint() *metaPrint {
	return &metaPrint{}
}

func (m *metaPrint) execute(cmd *CLICommand) (action, error) {
	cmd.Printf(cmd.buffer.print() + "\n")
	return actionNone, nil
}

// ////////////////////////////////////////////////////////////////////////////
// quit (or q)
// ////////////////////////////////////////////////////////////////////////////
type metaQuit struct{}

func newMetaQuit() *metaQuit {
	return &metaQuit{}
}

func (m *metaQuit) execute(cmd *CLICommand) (action, error) {
	return actionQuit, nil
}

// ////////////////////////////////////////////////////////////////////////////
// reset (or r)
// ////////////////////////////////////////////////////////////////////////////
type metaReset struct{}

func newMetaReset() *metaReset {
	return &metaReset{}
}

func (m *metaReset) execute(cmd *CLICommand) (action, error) {
	cmd.Printf(cmd.buffer.reset())
	return actionReset, nil
}

// ////////////////////////////////////////////////////////////////////////////
// set
// ////////////////////////////////////////////////////////////////////////////
type metaSet struct {
	args []string
}

func newMetaSet(args []string) *metaSet {
	return &metaSet{
		args: args,
	}
}

func (m *metaSet) execute(cmd *CLICommand) (action, error) {
	// TODO: set the variable (or clear it, etc)
	return actionNone, nil
}

//////////////////////////////////////////////////////////////////////////////

// splitMetaCommand takes a string which follows a backslash, with a
// formats like:
//
//	`cmd`
//	`cmd arg1 arg2`
//	`cmd 'arg1' arg2 'arg three'`
//
// It returns the metaCommand which maps to `cmd`.
func splitMetaCommand(in string) (metaCommand, error) {
	parts := strings.SplitN(in, ` `, 2)
	key := strings.TrimRightFunc(parts[0], unicode.IsSpace)

	args := []string{}
	if len(parts) > 1 {
		sb := &strings.Builder{}
		quoted := false
		for _, r := range parts[1] {
			if r == '\'' {
				quoted = !quoted
			} else if !quoted && r == ' ' {
				args = append(args, sb.String())
				sb.Reset()
			} else {
				sb.WriteRune(r)
			}
		}
		if sb.Len() > 0 {
			args = append(args, sb.String())
		}
	}

	switch key {
	case "!":
		return newMetaBang(args), nil
	case "cd":
		return newMetaChangeDirectory(args), nil
	case "c", "connect":
		return newMetaConnect(args), nil
	case "i", "include":
		return newMetaInclude(args), nil
	case "p", "print":
		return newMetaPrint(), nil
	case "q", "quit":
		return newMetaQuit(), nil
	case "r", "reset":
		return newMetaReset(), nil
	case "set":
		return newMetaSet(args), nil
	default:
		return nil, errors.Errorf("unsupported meta-command: '%s'", key)
	}
}
