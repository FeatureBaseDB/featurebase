package cli

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/featurebasedb/featurebase/v3/errors"
)

// responseAction is used to indicate how CLICommand should respond after
// executing a given metaCommand. For example, a responseAction of type "reset"
// tells CLICommand that the buffer has been reset and it needs to change its
// user prompt.
type responseAction string

const (
	actionNone  = ""
	actionQuit  = "quit"
	actionReset = "reset"
)

// metaCommand is the interface for any type responding to a "\" meta-command.
type metaCommand interface {
	execute(cmd *Command) (responseAction, error)
}

// Ensure type implements interface.
var _ metaCommand = (*metaBang)(nil)
var _ metaCommand = (*metaBorder)(nil)
var _ metaCommand = (*metaChangeDirectory)(nil)
var _ metaCommand = (*metaConnect)(nil)
var _ metaCommand = (*metaDescribe)(nil)
var _ metaCommand = (*metaEcho)(nil)
var _ metaCommand = (*metaExpanded)(nil)
var _ metaCommand = (*metaFile)(nil)
var _ metaCommand = (*metaHelp)(nil)
var _ metaCommand = (*metaInclude)(nil)
var _ metaCommand = (*metaListDatabases)(nil)
var _ metaCommand = (*metaListTables)(nil)
var _ metaCommand = (*metaListViews)(nil)
var _ metaCommand = (*metaOrg)(nil)
var _ metaCommand = (*metaOutput)(nil)
var _ metaCommand = (*metaPrint)(nil)
var _ metaCommand = (*metaPSet)(nil)
var _ metaCommand = (*metaQEcho)(nil)
var _ metaCommand = (*metaQuit)(nil)
var _ metaCommand = (*metaReset)(nil)
var _ metaCommand = (*metaSet)(nil)
var _ metaCommand = (*metaTiming)(nil)
var _ metaCommand = (*metaTuplesOnly)(nil)
var _ metaCommand = (*metaUnset)(nil)
var _ metaCommand = (*metaWarn)(nil)
var _ metaCommand = (*metaWatch)(nil)
var _ metaCommand = (*metaWrite)(nil)

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

func (m *metaBang) execute(cmd *Command) (responseAction, error) {
	if len(m.args) == 0 {
		return actionNone, errors.Errorf("meta command '!' requires at least one argument")
	}
	c := exec.Command(m.args[0])
	c.Args = m.args
	c.Stdout = cmd.stdout
	err := c.Run()
	return actionNone, errors.Wrap(err, "running bang command")
}

// ////////////////////////////////////////////////////////////////////////////
// border (sub-command of pset)
// ////////////////////////////////////////////////////////////////////////////
type metaBorder struct {
	args []string
}

func newMetaBorder(args []string) *metaBorder {
	return &metaBorder{
		args: args,
	}
}

func (m *metaBorder) execute(cmd *Command) (responseAction, error) {
	switch len(m.args) {
	case 0:
		// pass
	case 1:
		switch m.args[0] {
		case "1":
			cmd.writeOptions.border = 1
		case "2":
			cmd.writeOptions.border = 2
		default:
			cmd.writeOptions.border = 0
		}
	default:
		return actionNone, errors.Errorf("meta command 'border' takes zero or one argument")
	}

	cmd.Printf("Border style is %d.\n", cmd.writeOptions.border)
	return actionNone, nil
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

func (m *metaChangeDirectory) execute(cmd *Command) (responseAction, error) {
	var dir string
	switch len(m.args) {
	case 0:
		if d, err := os.UserHomeDir(); err != nil {
			return actionNone, errors.Wrapf(err, "getting home directory")
		} else {
			dir = d
		}
	case 1:
		dir = m.args[0]
	default:
		return actionNone, errors.Errorf("meta command 'cd' takes zero or one argument")
	}

	err := cmd.workingDir.cd(dir)
	return actionNone, errors.Wrapf(err, "changing directory to: %s", dir)
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

func (m *metaConnect) execute(cmd *Command) (responseAction, error) {
	switch len(m.args) {
	case 0:
		cmd.Printf(cmd.connectionMessage())
		return actionNone, nil
	case 1:
		err := cmd.connectToDatabase(m.args[0])
		return actionNone, err
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

func (m *metaEcho) execute(cmd *Command) (responseAction, error) {
	return echo(m.args, cmd.stdout)
}

func echo(args []string, w io.Writer) (responseAction, error) {
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
// expanded (x)
// ////////////////////////////////////////////////////////////////////////////
type metaExpanded struct {
	args []string
}

func newMetaExpanded(args []string) *metaExpanded {
	return &metaExpanded{
		args: args,
	}
}

func (m *metaExpanded) execute(cmd *Command) (responseAction, error) {
	switch len(m.args) {
	case 0:
		cmd.writeOptions.expanded = !cmd.writeOptions.expanded
	case 1:
		switch m.args[0] {
		case "on":
			cmd.writeOptions.expanded = true
		case "off":
			cmd.writeOptions.expanded = false
		default:
			return actionNone, errors.Errorf("unrecognized value \"%s\" for \"expanded\": Boolean expected", m.args[0])
		}
	default:
		return actionNone, errors.Errorf("meta command 'expanded' takes zero or one argument")
	}

	sExpanded := "on"
	if !cmd.writeOptions.expanded {
		sExpanded = "off"
	}

	cmd.Printf("Expanded display is %s.\n", sExpanded)
	return actionNone, nil
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

func (m *metaFile) execute(cmd *Command) (responseAction, error) {
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
// format (sub-command of pset)
// ////////////////////////////////////////////////////////////////////////////
type metaFormat struct {
	args []string
}

func newMetaFormat(args []string) *metaFormat {
	return &metaFormat{
		args: args,
	}
}

func (m *metaFormat) execute(cmd *Command) (responseAction, error) {
	switch len(m.args) {
	case 0:
		// pass
	case 1:
		switch m.args[0] {
		case formatAligned:
			cmd.writeOptions.format = formatAligned
		case formatCSV:
			cmd.writeOptions.format = formatCSV
		default:
			return actionNone, errors.Errorf("\\pset: allowed formats are aligned, csv")
		}
	default:
		return actionNone, errors.Errorf("meta command 'format' takes zero or one argument")
	}

	cmd.Printf("Output format is %s.\n", cmd.writeOptions.format)
	return actionNone, nil
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

func (m *metaHelp) execute(cmd *Command) (responseAction, error) {
	helpText := `General
  \q[uit]                quit psql
  \watch [SEC]           execute query every SEC seconds

Help
  \? [commands]          show help on backslash commands

Query Buffer
  \p[rint]               show the contents of the query buffer
  \r[eset]               reset (clear) the query buffer
  \w[rite] FILE          write query buffer to file

Input/Output
  \echo [-n] [STRING]    write string to standard output (-n for no newline)
  \file ...              reference a local file to stream to the server
  \i[nclude] FILE        execute commands from file
  \o[ut] [FILE]          send all query results to file
  \qecho [-n] [STRING]   write string to \o output stream (-n for no newline)
  \warn [-n] [STRING]    write string to standard error (-n for no newline)

Informational
  \d                     list tables, including system tables
  \d NAME                describe table
  \dt                    list tables
  \dv                    list views
  \l[ist]                list databases

Formatting
  \pset [NAME [VALUE]]   set table output option
                         (border|expanded|format|location|tuples_only)
  \t [on|off]            show only rows
  \x [on|off]            toggle expanded output

Connection
  \c[onnect] [DBNAME]    connect to new database
                         disconnect by sending DBNAME "-"
  \org [ORGNAME]         set organization id

Operating System
  \cd [DIR]              change the current working directory
  \timing [on|off]       toggle timing of commands
  \! [COMMAND]           execute command in shell or start interactive shell

Variables
  \set [NAME [VALUE]]    set internal variable, or list all if no parameters
  \unset NAME            unset (delete) internal variable
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

func (m *metaInclude) execute(cmd *Command) (responseAction, error) {
	if len(m.args) != 1 {
		return actionNone, errors.Errorf("meta command 'include' requires exactly one argument")
	}

	return executeFile(cmd, m.args[0])
}

func executeFile(cmd *Command, fileName string) (responseAction, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return actionNone, errors.Wrapf(err, "opening file: %s", fileName)
	}
	defer file.Close()

	splitter := newSplitter(newReplacer(cmd.variables))
	buffer := newBuffer()

	// Read the file by line, pushing the lines into a new line splitter, then
	// sending that output to a new buffer.
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		line := sc.Text() // GET the line string

		qps, mcs, err := splitter.split(line)
		if err != nil {
			return actionNone, errors.Wrapf(err, "splitting lines")
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

		for i := range mcs {
			action, err := mcs[i].execute(cmd)
			if err != nil {
				return actionNone, errors.Wrap(err, "executing meta command")
			}
			switch action {
			case actionQuit:
				close(cmd.quit)
				return action, nil
			}
		}
	}
	if err := sc.Err(); err != nil {
		return actionNone, errors.Wrapf(err, "scanning file: %s", fileName)
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

func (m *metaListDatabases) execute(cmd *Command) (responseAction, error) {
	qry := []queryPart{
		newPartRaw("SHOW DATABASES"),
	}

	if err := cmd.executeAndWriteQuery(qry); err != nil {
		return actionNone, errors.Wrap(err, "executing query")
	}

	return actionReset, nil
}

// ////////////////////////////////////////////////////////////////////////////
// describe (d)
// ////////////////////////////////////////////////////////////////////////////
type metaDescribe struct {
	args []string
}

func newMetaDescribe(args []string) *metaDescribe {
	return &metaDescribe{
		args: args,
	}
}

func (m *metaDescribe) execute(cmd *Command) (responseAction, error) {
	switch len(m.args) {
	case 0:
		// Describe with no args should list all relations (tables, views,
		// etc.). For now, we're just going to list the tables, including system
		// tables.
		qry := []queryPart{
			newPartRaw("SHOW TABLES WITH SYSTEM"),
		}

		if err := cmd.executeAndWriteQuery(qry); err != nil {
			return actionNone, errors.Wrap(err, "executing query")
		}

		return actionReset, nil

	case 1:
		// Describe with a single arg will assume the arg is a table name, so it
		// runs a `SHOW COLUMNS` for that table.
		qry := []queryPart{
			newPartRaw(fmt.Sprintf(`SHOW COLUMNS FROM "%s"`, m.args[0])),
		}

		if err := cmd.executeAndWriteQuery(qry); err != nil {
			return actionNone, errors.Wrap(err, "executing query")
		}

		return actionReset, nil
	default:
		return actionNone, errors.Errorf("meta command 'describe' takes zero or one argument")
	}
}

// ////////////////////////////////////////////////////////////////////////////
// describe (dt)
// ////////////////////////////////////////////////////////////////////////////
type metaListTables struct{}

func newMetaListTables() *metaListTables {
	return &metaListTables{}
}

func (m *metaListTables) execute(cmd *Command) (responseAction, error) {
	qry := []queryPart{
		newPartRaw("SHOW TABLES"),
	}

	if err := cmd.executeAndWriteQuery(qry); err != nil {
		return actionNone, errors.Wrap(err, "executing query")
	}

	return actionReset, nil
}

// ////////////////////////////////////////////////////////////////////////////
// describe views (dv)
// ////////////////////////////////////////////////////////////////////////////
type metaListViews struct{}

func newMetaListViews() *metaListViews {
	return &metaListViews{}
}

func (m *metaListViews) execute(cmd *Command) (responseAction, error) {
	qry := []queryPart{
		newPartRaw("SELECT * FROM fb_views"),
	}

	if err := cmd.executeAndWriteQuery(qry); err != nil {
		return actionNone, errors.Wrap(err, "executing query")
	}

	return actionReset, nil
}

// ////////////////////////////////////////////////////////////////////////////
// location (sub-command of pset)
// ////////////////////////////////////////////////////////////////////////////
type metaLocation struct {
	args []string
}

func newMetaLocation(args []string) *metaLocation {
	return &metaLocation{
		args: args,
	}
}

func (m *metaLocation) execute(cmd *Command) (responseAction, error) {
	switch len(m.args) {
	case 0:
		// pass
	case 1:
		loc, err := time.LoadLocation(m.args[0])
		if err != nil {
			return actionNone, errors.Wrapf(err, "loading location: %s", m.args[0])
		}
		cmd.writeOptions.location = loc
	default:
		return actionNone, errors.Errorf("meta command 'location' takes zero or one argument")
	}

	cmd.Printf("Location is %s.\n", cmd.writeOptions.location)
	return actionNone, nil
}

// ////////////////////////////////////////////////////////////////////////////
// org
// ////////////////////////////////////////////////////////////////////////////
type metaOrg struct {
	args []string
}

func newMetaOrg(args []string) *metaOrg {
	return &metaOrg{
		args: args,
	}
}

func (m *metaOrg) execute(cmd *Command) (responseAction, error) {
	switch len(m.args) {

	case 0:
		cmd.Printf(cmd.orgMessage())
		return actionNone, nil
	case 1:
		cmd.organizationID = m.args[0]
		cmd.Printf(cmd.orgMessage())
		return actionNone, nil

	default:
		return actionNone, errors.Errorf("meta command 'org' takes zero or one argument")
	}
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

func (m *metaOutput) execute(cmd *Command) (responseAction, error) {
	switch len(m.args) {
	case 0:
		// Close cmd.output (if closable).
		if err := cmd.closeOutput(); err != nil {
			return actionNone, errors.Wrapf(err, "closing output")
		}

		// Set cmd.output to cmd.Stdout.
		cmd.output = cmd.stdout

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

func (m *metaPrint) execute(cmd *Command) (responseAction, error) {
	cmd.Printf(cmd.buffer.print() + "\n")
	return actionNone, nil
}

// ////////////////////////////////////////////////////////////////////////////
// pset
// ////////////////////////////////////////////////////////////////////////////
type metaPSet struct {
	args []string
}

func newMetaPSet(args []string) *metaPSet {
	return &metaPSet{
		args: args,
	}
}

func (m *metaPSet) print(cmd *Command) {
	onOff := func(b bool) string {
		if b {
			return "on"
		}
		return "off"
	}

	fmt := `border      %d
expanded    %s
format      %s
location    %s
tuples_only %s
`

	cmd.Printf(fmt,
		cmd.writeOptions.border,
		onOff(cmd.writeOptions.expanded),
		cmd.writeOptions.format,
		cmd.writeOptions.location,
		onOff(cmd.writeOptions.tuplesOnly),
	)

}

func (m *metaPSet) execute(cmd *Command) (responseAction, error) {
	switch len(m.args) {
	case 0:
		m.print(cmd)
		return actionNone, nil
	case 1, 2:
		switch m.args[0] {
		case "border":
			sub := newMetaBorder(m.args[1:])
			return sub.execute(cmd)
		case "expanded", "x":
			sub := newMetaExpanded(m.args[1:])
			return sub.execute(cmd)
		case "format":
			sub := newMetaFormat(m.args[1:])
			return sub.execute(cmd)
		case "location":
			sub := newMetaLocation(m.args[1:])
			return sub.execute(cmd)
		case "tuples_only", "t":
			sub := newMetaTuplesOnly(m.args[1:])
			return sub.execute(cmd)
		default:
			return actionNone, errors.Errorf("unrecognized value \"%s\" for \"pset\"", m.args[0])
		}
	default:
		return actionNone, errors.Errorf("meta command 'pset' takes zero, one, or two arguments")
	}
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

func (m *metaQEcho) execute(cmd *Command) (responseAction, error) {
	return echo(m.args, cmd.output)
}

// ////////////////////////////////////////////////////////////////////////////
// quit (or q)
// ////////////////////////////////////////////////////////////////////////////
type metaQuit struct{}

func newMetaQuit() *metaQuit {
	return &metaQuit{}
}

func (m *metaQuit) execute(cmd *Command) (responseAction, error) {
	return actionQuit, nil
}

// ////////////////////////////////////////////////////////////////////////////
// reset (or r)
// ////////////////////////////////////////////////////////////////////////////
type metaReset struct{}

func newMetaReset() *metaReset {
	return &metaReset{}
}

func (m *metaReset) execute(cmd *Command) (responseAction, error) {
	cmd.Printf(cmd.buffer.reset() + "\n")
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

func (m *metaSet) execute(cmd *Command) (responseAction, error) {
	switch len(m.args) {
	case 0:
		// Sort the variables before printing them.
		keys := make([]string, 0, len(cmd.variables))
		for k := range cmd.variables {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		// Print out the variables.
		for _, k := range keys {
			cmd.Printf("%s = '%s'\n", k, cmd.variables[k])
		}
		cmd.writeOptions.timing = !cmd.writeOptions.timing
	default:
		// The first arg is the key, the remaining args are concatenated together to form the values
		// For example:
		// \set one two three
		// will result in `one = 'twothree'`
		k := m.args[0]
		v := strings.Join(m.args[1:], "")
		cmd.variables[k] = v
	}
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

func (m *metaTiming) execute(cmd *Command) (responseAction, error) {
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
			return actionNone, errors.Errorf("unrecognized value \"%s\" for \"\\timing\": Boolean expected", m.args[0])
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
// tuples_only (t)
// ////////////////////////////////////////////////////////////////////////////
type metaTuplesOnly struct {
	args []string
}

func newMetaTuplesOnly(args []string) *metaTuplesOnly {
	return &metaTuplesOnly{
		args: args,
	}
}

func (m *metaTuplesOnly) execute(cmd *Command) (responseAction, error) {
	switch len(m.args) {
	case 0:
		cmd.writeOptions.tuplesOnly = !cmd.writeOptions.tuplesOnly
	case 1:
		switch m.args[0] {
		case "on":
			cmd.writeOptions.tuplesOnly = true
		case "off":
			cmd.writeOptions.tuplesOnly = false
		default:
			return actionNone, errors.Errorf("unrecognized value \"%s\" for \"tuples_only\": Boolean expected", m.args[0])
		}
	default:
		return actionNone, errors.Errorf("meta command 'tuples_only' takes zero or one argument")
	}

	sTuplesOnly := "on"
	if !cmd.writeOptions.tuplesOnly {
		sTuplesOnly = "off"
	}

	cmd.Printf("Tuples only is %s.\n", sTuplesOnly)
	return actionNone, nil
}

// ////////////////////////////////////////////////////////////////////////////
// unset
// ////////////////////////////////////////////////////////////////////////////
type metaUnset struct {
	args []string
}

func newMetaUnset(args []string) *metaUnset {
	return &metaUnset{
		args: args,
	}
}

func (m *metaUnset) execute(cmd *Command) (responseAction, error) {
	switch len(m.args) {
	case 0:
		cmd.Printf("\\unset: missing required argument\n")
		return actionNone, nil
	default:
		if len(m.args) > 1 {
			for _, s := range m.args[1:] {
				cmd.Printf("\\unset: extra argument \"%s\" ignored\n", s)
			}
		}
		delete(cmd.variables, m.args[0])
	}
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

func (m *metaWarn) execute(cmd *Command) (responseAction, error) {
	return echo(m.args, cmd.stderr)
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

func (m *metaWatch) execute(cmd *Command) (responseAction, error) {
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

// ////////////////////////////////////////////////////////////////////////////
// write (w)
// ////////////////////////////////////////////////////////////////////////////
type metaWrite struct {
	args []string
}

func newMetaWrite(args []string) *metaWrite {
	return &metaWrite{
		args: args,
	}
}

func (m *metaWrite) execute(cmd *Command) (responseAction, error) {
	switch len(m.args) {
	case 0:
		cmd.Errorf(`\w: missing required argument` + "\n")
		return actionNone, nil

	case 1:
		// Open file.
		fpath, err := filepath.Abs(m.args[0])
		if err != nil {
			return actionNone, errors.Wrapf(err, "getting absolute file path for file: %s", m.args[0])
		}

		file, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			return actionNone, errors.Wrapf(err, "opening file: %s", fpath)
		}
		defer file.Close()

		// Write query buffer to file.
		if _, err := io.Copy(file, cmd.buffer.Reader()); err != nil {
			return actionNone, errors.Wrapf(err, "writing query buffer to file: %s", fpath)
		}

		return actionNone, nil

	default:
		return actionNone, errors.Errorf("meta command 'w' exactly one argument")
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
func splitMetaCommand(in string, replacer *replacer) (metaCommand, error) {
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

	// Do variable replacement.
	for i := range args {
		args[i] = replacer.replace(args[i])
	}

	switch key {
	case "!":
		return newMetaBang(args), nil
	case "cd":
		return newMetaChangeDirectory(args), nil
	case "c", "connect":
		return newMetaConnect(args), nil
	case "d":
		return newMetaDescribe(args), nil
	case "dt":
		return newMetaListTables(), nil
	case "dv":
		return newMetaListViews(), nil
	case "echo":
		return newMetaEcho(args), nil
	case "file":
		return newMetaFile(args), nil
	case "?":
		return newMetaHelp(args), nil
	case "i", "include":
		return newMetaInclude(args), nil
	case "l", "list":
		return newMetaListDatabases(), nil
	case "o", "out":
		return newMetaOutput(args), nil
	case "org":
		return newMetaOrg(args), nil
	case "p", "print":
		return newMetaPrint(), nil
	case "pset":
		return newMetaPSet(args), nil
	case "qecho":
		return newMetaQEcho(args), nil
	case "q", "quit":
		return newMetaQuit(), nil
	case "r", "reset":
		return newMetaReset(), nil
	case "set":
		return newMetaSet(args), nil
	case "t":
		return newMetaTuplesOnly(args), nil
	case "timing":
		return newMetaTiming(args), nil
	case "unset":
		return newMetaUnset(args), nil
	case "warn":
		return newMetaWarn(args), nil
	case "watch":
		return newMetaWatch(args), nil
	case "w", "write":
		return newMetaWrite(args), nil
	case "x":
		return newMetaExpanded(args), nil
	default:
		return nil, errors.Errorf("unsupported meta-command: '%s'", key)
	}
}
