package cli

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
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
var _ metaCommand = (*metaEcho)(nil)
var _ metaCommand = (*metaFile)(nil)
var _ metaCommand = (*metaHelp)(nil)
var _ metaCommand = (*metaInclude)(nil)
var _ metaCommand = (*metaListDatabases)(nil)
var _ metaCommand = (*metaListTables)(nil)
var _ metaCommand = (*metaOutput)(nil)
var _ metaCommand = (*metaPrint)(nil)
var _ metaCommand = (*metaQEcho)(nil)
var _ metaCommand = (*metaQuit)(nil)
var _ metaCommand = (*metaReset)(nil)
var _ metaCommand = (*metaSet)(nil)
var _ metaCommand = (*metaTiming)(nil)
var _ metaCommand = (*metaWarn)(nil)
var _ metaCommand = (*metaWatch)(nil)

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
	msg := func() string {
		return fmt.Sprintf("You are now connected to database \"%s\" (%s) as user \"???\".\n", cmd.DatabaseName, cmd.DatabaseID)
	}

	switch len(m.args) {
	case 0:
		if cmd.DatabaseName == "" {
			cmd.Printf("You are not connected to a database.\n")
		} else {
			cmd.Printf(msg())
		}
		return actionNone, nil
	case 1:
		dbName := m.args[0]

		// Look up dbID based on dbName.
		qry := []queryPart{
			newPartRaw("SHOW DATABASES"),
		}

		qr, err := cmd.executeQuery(qry)
		if err != nil {
			return actionNone, errors.Wrap(err, "executing query")
		}

		for _, db := range qr.Data {
			// 0: _id
			// 1: name
			if db[1] == dbName {
				cmd.DatabaseName = dbName
				cmd.DatabaseID = db[0].(string)
				cmd.Printf(msg())
				return actionReset, nil
			}
		}
		return actionNone, errors.Errorf("invalid database: %s", dbName)

	default:
		return actionNone, errors.Errorf("meta command 'connect' takes zero or one argument")
	}
}

// ////////////////////////////////////////////////////////////////////////////
// echo
// ////////////////////////////////////////////////////////////////////////////
type metaEcho struct {
	args []string
}

func newMetaEcho(args []string) *metaEcho {
	return &metaEcho{
		args: args,
	}
}

func (m *metaEcho) execute(cmd *CLICommand) (action, error) {
	return echo(m.args, cmd.Stdout)
}

func echo(args []string, w io.Writer) (action, error) {
	switch len(args) {
	case 0:
		w.Write([]byte("\n"))
		return actionNone, nil
	default:
		var s string
		switch args[0] {
		// TODO(tlt): currently the "-n" doesn't *appear* to work because
		// the readline package, on its next iteration of the read loop,
		// clobbers anything written to the current line (i.e. anything
		// without a line feed). We need to figure out how to keep the
		// contents of the current line, and append the next readline prompt
		// to the end of it.
		case "-n":
			s = strings.Join(args[1:], " ")
		default:
			s = strings.Join(args, " ") + "\n"
		}
		w.Write([]byte(s))
		return actionNone, nil
	}
}

// ////////////////////////////////////////////////////////////////////////////
// file
// ////////////////////////////////////////////////////////////////////////////
type metaFile struct {
	args []string
}

func newMetaFile(args []string) *metaFile {
	return &metaFile{
		args: args,
	}
}

func (m *metaFile) execute(cmd *CLICommand) (action, error) {
	if len(m.args) != 1 {
		return actionNone, errors.Errorf("meta command 'file' requires exactly one argument")
	}

	// TODO(tlt): I think instead of opening the file here, we should get the
	// absolute path to the file and store that until we actually need to open
	// the file (i.e until we actualy execute the query). But to do that we'll
	// need to change the Reader() method to return an error.
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
// help (?)
// ////////////////////////////////////////////////////////////////////////////
type metaHelp struct {
	args []string
}

func newMetaHelp(args []string) *metaHelp {
	return &metaHelp{
		args: args,
	}
}

func (m *metaHelp) execute(cmd *CLICommand) (action, error) {
	helpText := `General
  \q[uit]                quit psql
  \watch [SEC]           execute query every SEC seconds

Help
  \? [commands]          show help on backslash commands

Query Buffer
  \p[rint]               show the contents of the query buffer
  \r[eset]               reset (clear) the query buffer

Input/Output
  \echo [-n] [STRING]    write string to standard output (-n for no newline)
  \file ...              reference a local file to stream to the server
  \i[nclude] FILE        execute commands from file
  \o [FILE]              send all query results to file
  \qecho [-n] [STRING]   write string to \o output stream (-n for no newline)
  \warn [-n] [STRING]    write string to standard error (-n for no newline)

Informational
  \d                     list tables and views
  \dt                    list tables
  \dv                    list views
  \l                     list databases

Connection
  \c[onnect] [DBNAME]    connect to new database

Operating System
  \cd [DIR]              change the current working directory
  \timing [on|off]       toggle timing of commands
  \! [COMMAND]           execute command in shell or start interactive shell
`
	cmd.Printf("%s\n", helpText)

	return actionNone, nil
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
	defer file.Close()

	splitter := newSplitter()
	buffer := newBuffer()

	// Read the file by line, pushing the lines into a new line splitter, then
	// sending that output to a new buffer.
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		line := sc.Text() // GET the line string

		qps, mcs, err := splitter.split(line)
		if err != nil {
			return actionNone, errors.Wrapf(err, "splitting lines")
		} else if len(mcs) > 0 {
			return actionNone, errors.Errorf("include does  not support meta-commands")
		}

		for i := range qps {
			if qry, err := buffer.addPart(qps[i]); err != nil {
				return actionNone, errors.Wrap(err, "adding part to buffer")
			} else if qry != nil {
				if err := cmd.executeAndWriteQuery(qry); err != nil {
					return actionNone, errors.Wrap(err, "executing query")
				}
			}
		}
	}
	if err := sc.Err(); err != nil {
		return actionNone, errors.Wrapf(err, "scanning file: %s", m.args[0])
	}

	return actionReset, nil
}

// ////////////////////////////////////////////////////////////////////////////
// list databases (l)
// ////////////////////////////////////////////////////////////////////////////
type metaListDatabases struct{}

func newMetaListDatabases() *metaListDatabases {
	return &metaListDatabases{}
}

func (m *metaListDatabases) execute(cmd *CLICommand) (action, error) {
	qry := []queryPart{
		newPartRaw("SHOW DATABASES"),
	}

	if err := cmd.executeAndWriteQuery(qry); err != nil {
		return actionNone, errors.Wrap(err, "executing query")
	}

	return actionReset, nil
}

// ////////////////////////////////////////////////////////////////////////////
// list tables (d or dt)
// ////////////////////////////////////////////////////////////////////////////
type metaListTables struct{}

func newMetaListTables() *metaListTables {
	return &metaListTables{}
}

func (m *metaListTables) execute(cmd *CLICommand) (action, error) {
	qry := []queryPart{
		newPartRaw("SHOW TABLES"),
	}

	if err := cmd.executeAndWriteQuery(qry); err != nil {
		return actionNone, errors.Wrap(err, "executing query")
	}

	return actionReset, nil
}

// ////////////////////////////////////////////////////////////////////////////
// output (o)
// ////////////////////////////////////////////////////////////////////////////
type metaOutput struct {
	args []string
}

func newMetaOutput(args []string) *metaOutput {
	return &metaOutput{
		args: args,
	}
}

func (m *metaOutput) execute(cmd *CLICommand) (action, error) {
	switch len(m.args) {
	case 0:
		// Close cmd.output (if closable).
		if err := cmd.closeOutput(); err != nil {
			return actionNone, errors.Wrapf(err, "closing output")
		}

		// Set cmd.output to cmd.Stdout.
		cmd.output = cmd.Stdout

		// cmd.writer = newStandardWriter(Stdout, Stderr)
		return actionNone, nil
	case 1:
		// If the argument is a fully-qualifed file path, just use that.
		// Otherwise, prepend it with the current working directory.
		fpath, err := filepath.Abs(m.args[0])
		if err != nil {
			return actionNone, errors.Wrapf(err, "getting absolute file path for file: %s", m.args[0])
		}

		cmd.output, err = os.OpenFile(fpath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
		if err != nil {
			return actionNone, errors.Wrapf(err, "opening file: %s", fpath)
		}

		// cmd.writer = newFileWriter(fpath, Stdout, Stderr)

		return actionNone, nil
	default:
		return actionNone, errors.Errorf("meta command 'output' takes zero or one argument")
	}
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
// qecho
// ////////////////////////////////////////////////////////////////////////////
type metaQEcho struct {
	args []string
}

func newMetaQEcho(args []string) *metaQEcho {
	return &metaQEcho{
		args: args,
	}
}

func (m *metaQEcho) execute(cmd *CLICommand) (action, error) {
	return echo(m.args, cmd.output)
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

// ////////////////////////////////////////////////////////////////////////////
// timing
// ////////////////////////////////////////////////////////////////////////////
type metaTiming struct {
	args []string
}

func newMetaTiming(args []string) *metaTiming {
	return &metaTiming{
		args: args,
	}
}

func (m *metaTiming) execute(cmd *CLICommand) (action, error) {
	switch len(m.args) {
	case 0:
		cmd.writeOptions.timing = !cmd.writeOptions.timing
	case 1:
		switch m.args[0] {
		case "on":
			cmd.writeOptions.timing = true
		case "off":
			cmd.writeOptions.timing = false
		default:
			return actionNone, errors.Errorf("unrecognized value \"%s\" for \"\timing\": Boolean expected", m.args[0])
		}
	default:
		return actionNone, errors.Errorf("meta command 'timing' takes zero or one argument")
	}

	sTiming := "on"
	if !cmd.writeOptions.timing {
		sTiming = "off"
	}

	cmd.Printf("Timing is %s.\n", sTiming)
	return actionNone, nil
}

// ////////////////////////////////////////////////////////////////////////////
// warn
// ////////////////////////////////////////////////////////////////////////////
type metaWarn struct {
	args []string
}

func newMetaWarn(args []string) *metaWarn {
	return &metaWarn{
		args: args,
	}
}

func (m *metaWarn) execute(cmd *CLICommand) (action, error) {
	return echo(m.args, cmd.Stderr)
}

// ////////////////////////////////////////////////////////////////////////////
// watch
// ////////////////////////////////////////////////////////////////////////////
type metaWatch struct {
	args []string
}

func newMetaWatch(args []string) *metaWatch {
	return &metaWatch{
		args: args,
	}
}

func (m *metaWatch) execute(cmd *CLICommand) (action, error) {
	period := 2 * time.Second

	qry := cmd.buffer.lastQuery
	if qry == nil {
		cmd.Errorf(`\watch cannot be used with an empty query` + "\n")
		return actionNone, nil
	}

	switch len(m.args) {
	case 1:
		val, err := strconv.Atoi(m.args[0])
		if err != nil {
			return actionNone, errors.Errorf("invalid watch argument: %s", m.args[0])
		}
		period = time.Duration(val) * time.Second
		fallthrough
	case 0:
		// Listen for a SIGTERM to cancel out of the \watch loop.
		controlC := make(chan os.Signal, 2)
		signal.Notify(controlC, os.Interrupt, syscall.SIGTERM)

		// In the absence of a SIGTERM, use a ticker to execute the query every
		// "period" duration.
		ticker := time.NewTicker(period)
		for {
			cmd.Printf("%s (every %s)\n\n", time.Now(), period)
			if err := cmd.executeAndWriteQuery(qry); err != nil {
				return actionNone, errors.Wrap(err, "executing query")
			}
			select {
			case <-controlC:
				return actionNone, nil
			case <-ticker.C:
			}
		}
	default:
		return actionNone, errors.Errorf("meta command 'watch' takes zero or one argument")
	}
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
	case "d", "dt":
		return newMetaListTables(), nil
	case "echo":
		return newMetaEcho(args), nil
	case "file":
		return newMetaFile(args), nil
	case "?":
		return newMetaHelp(args), nil
	case "i", "include":
		return newMetaInclude(args), nil
	case "l":
		return newMetaListDatabases(), nil
	case "o":
		return newMetaOutput(args), nil
	case "p", "print":
		return newMetaPrint(), nil
	case "qecho":
		return newMetaQEcho(args), nil
	case "q", "quit":
		return newMetaQuit(), nil
	case "r", "reset":
		return newMetaReset(), nil
	case "set":
		return newMetaSet(args), nil
	case "timing":
		return newMetaTiming(args), nil
	case "warn":
		return newMetaWarn(args), nil
	case "watch":
		return newMetaWatch(args), nil
	default:
		return nil, errors.Errorf("unsupported meta-command: '%s'", key)
	}
}
