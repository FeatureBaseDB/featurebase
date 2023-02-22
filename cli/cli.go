// Package cli contains a FeatureBase command line interface.
package cli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chzyer/readline"
	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/cli/fbcloud"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

const (
	defaultHost     string = "localhost"
	promptBegin     string = "fbsql> "
	promptMid       string = "    -> "
	terminationChar string = ";"
	nullValue       string = "NULL"
)

var (
	Stdin  io.ReadCloser = os.Stdin
	Stdout io.Writer     = os.Stdout
	Stderr io.Writer     = os.Stderr
)

var (
	splash string = fmt.Sprintf(`FeatureBase CLI (%s)
Type "\q" to quit.
`, featurebase.Version)
)

// Ensure type implments interfaces.
var _ printer = (*Command)(nil)

type Command struct {
	host string
	port string

	splitter   *splitter
	buffer     *buffer
	workingDir *workingDir

	organizationID string
	database       string
	databaseID     string
	databaseName   string

	Queryer Queryer `json:"-"`

	Stdin  io.ReadCloser `json:"-"`
	Stdout io.Writer     `json:"-"`
	Stderr io.Writer     `json:"-"`

	// output is where actual results are written. This might point to stdout,
	// or to a file, based on the current configuration.
	output       io.Writer `json:"-"`
	writeOptions *writeOptions

	Config *Config `json:"config"`

	historyPath string

	// Commands contains optional commands provided via one or more `-c` (or
	// `--command`) flags. If this is non-empty, the cli will run in
	// non-interactive mode; i.e. it will quit after the command is complete.
	Commands []string `json:"commands"`

	// Files contains optional files provided via one or more `-f` (or `--file`)
	// flags. If this is non-empty, the cli will run in non-interactive mode;
	// i.e. it will quit after the command is complete.
	Files []string `json:"files"`

	// variables holds the variables created with the \set meta-command.
	variables map[string]string

	// nonInteractiveMode is set to true when fbsql is running in
	// non-ineracative mode. And example of this is when the user has provided a
	// `-c` flag in the command line.
	nonInteractiveMode bool

	// quit gets closed when Run should stop listening for input.
	quit chan struct{}
}

func NewCommand(logdest logger.Logger) *Command {
	variables := make(map[string]string)

	return &Command{
		Config: &Config{
			Host: defaultHost,
			Port: "",

			OrganizationID: "",
			Database:       "",

			CloudAuth: CloudAuthConfig{
				ClientID: "",
				Region:   "",
				Email:    "",
				Password: "",
			},

			HistoryPath: "",
		},

		buffer:     newBuffer(),
		splitter:   newSplitter(newReplacer(variables)),
		workingDir: newWorkingDir(),

		Stdin:  Stdin,
		Stdout: Stdout,
		Stderr: Stderr,

		output:       Stdout,
		writeOptions: defaultWriteOptions(),

		variables: variables,

		quit: make(chan struct{}),
	}
}

// Run is the main entry-point to the CLI.
func (cmd *Command) Run(ctx context.Context) error {
	cmd.setupConfig()

	// Check to see if Command needs to run in non-interactive mode.
	if len(cmd.Commands) > 0 || len(cmd.Files) > 0 {
		cmd.nonInteractiveMode = true

		if err := cmd.setupClient(); err != nil {
			return errors.Wrap(err, "setting up client")
		}
		if err := cmd.connectToDatabase(cmd.database); err != nil {
			cmd.Errorf(errors.Wrap(err, "connecting to database").Error() + "\n")
		}

		// Run Commands.
		for _, line := range cmd.Commands {
			if err := cmd.handleLine(line); err != nil {
				cmd.Errorf(err.Error())
				return nil
			}
		}

		// Run Files.
		for _, fname := range cmd.Files {
			if _, err := executeFile(cmd, fname); err != nil {
				cmd.Errorf(err.Error())
				return nil
			}
		}

		return nil
	}

	// Print the splash message.
	cmd.Printf(splash)
	cmd.setupHistory()
	if err := cmd.setupClient(); err != nil {
		return errors.Wrap(err, "setting up client")
	}
	cmd.printConnInfo()
	if err := cmd.connectToDatabase(cmd.database); err != nil {
		cmd.Errorf(errors.Wrap(err, "connecting to database").Error() + "\n")
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 promptBegin,
		HistoryFile:            cmd.historyPath,
		HistoryLimit:           100000,
		DisableAutoSaveHistory: true,

		Stdin:  cmd.Stdin,
		Stdout: cmd.Stdout,
		Stderr: cmd.Stderr,
	})
	if err != nil {
		return errors.Wrap(err, "getting readline")
	}
	defer rl.Close()

	// inMidCommand indicates whether a partial command has been received and
	// we're still waiting for a termination character.
	var inMidCommand bool

	for {
		if inMidCommand {
			rl.SetPrompt(promptMid)
		} else {
			rl.SetPrompt(promptBegin)
		}

		// Read user provided input.
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			inMidCommand = false
			cmd.buffer.reset()
			continue
		} else if err != nil {
			return errors.Wrap(err, "reading line")
		}

		// We append a line feed at the end of each line because at this point
		// we have effectively stripped any intentional line feeds (since we are
		// reading a line at a time), and we don't want to do that. An example
		// of an intentional line feed is in a BULK INSERT CSV STREAM like this
		// example:
		//
		// bulk replace
		// 	 into foo (_id, age)
		// 	 map (0 id, 1 int)
		// from
		// 	 x'3,33
		// 	 4,44
		// 	 5,55'
		// with
		// 	 format 'CSV'
		// 	 input 'STREAM';
		//
		// We want to preserve the line feeds that are contained in the x''
		// block; those are intentional as they demarc records within the csv.
		qps, mcs, err := cmd.splitter.split(line + "\n")
		if err != nil {
			cmd.Errorf("error splitting line: %s\n", err)
			continue
		}

		// Save line in the history.
		if err := rl.SaveHistory(line); err != nil {
			cmd.Errorf("Couldn't save history: %v\n", err)
		}

		// This is wrapped in an anonymous function so we can capture any
		// errors, ignore the rest of the line, and return back to a prompt.
		if err := func() error {
			for i := range qps {
				if qry, err := cmd.buffer.addPart(qps[i]); err != nil {
					return errors.Wrap(err, "adding part to buffer")
				} else if qry != nil {
					if err := cmd.executeAndWriteQuery(qry); err != nil {
						return errors.Wrap(err, "executing query")
					}
					// In addition to saving each line in the history, we also
					// save each successful query.
					if err := rl.SaveHistory(qry.String() + ";"); err != nil {
						cmd.Errorf("Couldn't save query in history: %v\n", err)
					}
					inMidCommand = false
				} else {
					inMidCommand = true
				}
			}
			return nil
		}(); err != nil {
			cmd.Errorf(err.Error() + "\n")
			inMidCommand = false
			continue
		}

		// This is wrapped in an anonymous function so we can capture any
		// errors, ignore the rest of the line, and return back to a prompt.
		if err := func() error {
			for i := range mcs {
				action, err := mcs[i].execute(cmd)
				if err != nil {
					return errors.Wrap(err, "executing meta command")
				}
				switch action {
				case actionQuit:
					close(cmd.quit)
					return nil
				case actionReset:
					inMidCommand = false
				}
			}
			return nil
		}(); err != nil {
			cmd.Errorf(err.Error() + "\n")
			inMidCommand = false
			continue
		}

		select {
		case <-cmd.quit:
			if err := cmd.close(); err != nil {
				cmd.Errorf("closing: %s\n", err)
			}
			return nil
		default:
			//pass
		}
	}
}

// close is called upon quitting. It should close any remaining open file
// handles used by the CLICommand.
func (cmd *Command) close() error {
	return cmd.closeOutput()
}

// setupConfig sets up private struct members based on values provided via the
// configuration flags.
func (cmd *Command) setupConfig() {
	if cmd.Config == nil {
		return
	}

	cmd.host = cmd.Config.Host
	cmd.port = cmd.Config.Port

	cmd.organizationID = cmd.Config.OrganizationID
	cmd.database = cmd.Config.Database

	cmd.historyPath = cmd.Config.HistoryPath
}

func (cmd *Command) executeAndWriteQuery(qry query) error {
	queryResponse, err := cmd.executeQuery(qry)
	if err != nil {
		return errors.Wrap(err, "making query")
	}

	if err := writeTable(queryResponse, cmd.writeOptions, cmd.output, cmd.Stdout, cmd.Stderr); err != nil {
		return errors.Wrap(err, "writing out response")
	}

	return nil
}

func (cmd *Command) executeQuery(qry query) (*featurebase.WireQueryResponse, error) {
	wqr, err := cmd.Queryer.Query(cmd.organizationID, cmd.databaseID, qry.Reader())
	if err != nil {
		return nil, errors.Wrap(err, "executing query")
	}

	// If we're running in non-interactive mode, we need to check the error that
	// comes back in the WireQueryResponse. If there's an error, we want to
	// return it now (rather than just printing it later) so that we immediately
	// stop any further execution of commands.
	if cmd.nonInteractiveMode && wqr.Error != "" {
		return nil, errors.Errorf(wqr.Error)
	}

	return wqr, nil
}

// printer is an interface which encapsulates the methods used to print output
// to the various io.Writers.
type printer interface {
	Printf(format string, a ...any)
	Outputf(format string, a ...any)
	Errorf(format string, a ...any)
}

type nopPrinter struct{}

func newNopPrinter() *nopPrinter {
	return &nopPrinter{}
}

func (n *nopPrinter) Printf(format string, a ...any)  {}
func (n *nopPrinter) Outputf(format string, a ...any) {}
func (n *nopPrinter) Errorf(format string, a ...any)  {}

// Printf is a helper method which sends the given payload to stdout.
func (cmd *Command) Printf(format string, a ...any) {
	out := fmt.Sprintf(format, a...)
	cmd.Stdout.Write([]byte(out))
}

// Outputf is a helper method which sends the given payload to output.
func (cmd *Command) Outputf(format string, a ...any) {
	out := fmt.Sprintf(format, a...)
	cmd.output.Write([]byte(out))
}

// Errorf is a helper method which sends the given payload to stderr.
func (cmd *Command) Errorf(format string, a ...any) {
	out := fmt.Sprintf(format, a...)
	cmd.Stderr.Write([]byte(out))
}

func (cmd *Command) setupHistory() {
	// If HistoryPath has already been configured (i.e. with a command flag),
	// don't bother setting up the default in the home directory.
	if cmd.historyPath != "" {
		return
	}

	historyPath := ""
	if home, err := os.UserHomeDir(); err != nil {
		cmd.Errorf("Error getting home directory, command history persistence will be disabled: %v\n", err)
	} else {
		historyDir := filepath.Join(home, ".featurebase")
		err := os.MkdirAll(historyDir, 0o750)
		if err != nil {
			cmd.Errorf("Creating directory for history: %v\n", err)
		} else {
			historyPath = filepath.Join(historyDir, "cli_history")
		}
	}
	cmd.historyPath = historyPath
}

// printConnInfo displays the currently set host.
// TODO(tlt): extend this to be the output of the /conninfo meta-command.
func (cmd *Command) printConnInfo() {
	cmd.Printf("Host: %s\n", hostPort(cmd.host, cmd.port))
}

func (cmd *Command) connectToDatabase(dbName string) error {
	var p printer = cmd
	if cmd.nonInteractiveMode {
		p = newNopPrinter()
	}

	if dbName == "" {
		cmd.databaseID = ""
		cmd.databaseName = ""
		p.Printf(cmd.connectionMessage())
		return nil
	}

	// Look up dbID based on dbName.
	qry := []queryPart{
		newPartRaw("SHOW DATABASES"),
	}

	qr, err := cmd.executeQuery(qry)
	if err != nil {
		return errors.Wrap(err, "executing query")
	}

	for _, db := range qr.Data {
		// 0: _id
		// 1: name
		if db[1] == dbName {
			cmd.databaseName = dbName
			cmd.databaseID = db[0].(string)
			p.Printf(cmd.connectionMessage())
			return nil
		}
	}
	return errors.Errorf("invalid database: %s", dbName)
}

func (cmd *Command) orgMessage() string {
	if cmd.organizationID == "" {
		return "You have not set an organization.\n"
	}
	return fmt.Sprintf("You have set organization \"%s\".\n", cmd.organizationID)
}

func (cmd *Command) connectionMessage() string {
	if cmd.databaseName == "" {
		return "You are not connected to a database.\n"
	}
	return fmt.Sprintf("You are now connected to database \"%s\" (%s) as user \"???\".\n", cmd.databaseName, cmd.databaseID)
}

func (cmd *Command) setupClient() error {
	// If the Queryer has already been set (in tests for example), don't bother
	// trying to detect it.
	if cmd.Queryer != nil {
		return nil
	}

	var p printer = cmd
	if cmd.nonInteractiveMode {
		p = newNopPrinter()
	}

	if strings.TrimSpace(cmd.host) == "" {
		return errors.Errorf("no host provided\n")
	}

	if !strings.HasPrefix(cmd.host, "http") {
		cmd.host = "http://" + cmd.host
	}

	typ, err := cmd.detectFBType()
	if err != nil {
		return errors.Wrap(err, "detecting FeatureBase deployment type")
	}

	switch typ {
	case featurebaseTypeOnPremClassic:
		p.Printf("Detected on-prem, classic deployment.\n")
		cmd.Queryer = &standardQueryer{
			Host: cmd.host,
			Port: cmd.port,
		}
	case featurebaseTypeOnPremServerless:
		p.Printf("Detected on-prem, serverless deployment.\n")
		cmd.Queryer = &serverlessQueryer{
			Host: cmd.host,
			Port: cmd.port,
		}
	case featurebaseTypeCloud:
		p.Printf("Detected cloud deployment.\n")

		cmd.Queryer = &fbcloud.Queryer{
			Host: hostPort(cmd.host, cmd.port),

			ClientID: cmd.Config.CloudAuth.ClientID,
			Region:   cmd.Config.CloudAuth.Region,
			Email:    cmd.Config.CloudAuth.Email,
			Password: cmd.Config.CloudAuth.Password,
		}
	case featurebaseTypeUnknown:
		p.Printf("Could not detect deployment\n")
		// cmd.Queryer = &nopQueryer{}
		// Instead of using a no-op queryer when the type can't be detected, we
		// default to using a cloud queryer.
		cmd.Queryer = &fbcloud.Queryer{
			Host: hostPort(cmd.host, cmd.port),

			ClientID: cmd.Config.CloudAuth.ClientID,
			Region:   cmd.Config.CloudAuth.Region,
			Email:    cmd.Config.CloudAuth.Email,
			Password: cmd.Config.CloudAuth.Password,
		}
	default:
		return errors.Errorf("unknown type: %s", typ)
	}
	return nil
}

type featurebaseType string

const (
	featurebaseTypeUnknown          featurebaseType = "unknown"            // unknown
	featurebaseTypeOnPremClassic    featurebaseType = "on-prem-standard"   // on-prem, classic
	featurebaseTypeOnPremServerless featurebaseType = "on-prem-serverless" // on-prem, serverless
	featurebaseTypeCloud            featurebaseType = "cloud"              // cloud, (both classic and serverless)?
)

func hostPort(host, port string) string {
	if port == "" {
		return host
	}
	return host + ":" + port
}

// detectFBType determines if we're talking to standalone FeatureBase
// or FeatureBase Cloud
func (cmd *Command) detectFBType() (featurebaseType, error) {
	type trial struct {
		port   string
		health string
		typ    featurebaseType
	}

	// trials is populated with the url/endpoints to try in order to detect if a
	// process is running there which can support the cli requests.
	trials := []trial{}

	var clientTimeout time.Duration
	if cmd.port != "" {
		clientTimeout = 100 * time.Millisecond
		trials = append(trials,
			// on-prem, serverless
			trial{
				port:   cmd.port,
				health: "/queryer/health",
				typ:    featurebaseTypeOnPremServerless,
			},
			// on-prem, classic
			trial{
				port:   cmd.port,
				health: "/status",
				typ:    featurebaseTypeOnPremClassic,
			},
		)
	} else if strings.HasPrefix(cmd.host, "https") {
		// https suggesting we might be connecting to a cloud host
		clientTimeout = 1 * time.Second
		trials = append(trials,
			// cloud
			trial{
				port:   "",
				health: "/health",
				typ:    featurebaseTypeCloud,
			},
		)
	} else {
		// Try default ports just in case.
		clientTimeout = 100 * time.Millisecond
		trials = append(trials,
			// on-prem, serverless
			trial{
				port:   "8080",
				health: "/queryer/health",
				typ:    featurebaseTypeOnPremServerless,
			},
			// on-prem, classic
			trial{
				port:   "10101",
				health: "/status",
				typ:    featurebaseTypeOnPremClassic,
			},
		)
	}

	client := http.Client{
		Timeout: clientTimeout,
	}
	for _, trial := range trials {
		url := hostPort(cmd.host, trial.port) + trial.health
		if resp, err := client.Get(url); err != nil {
			continue
		} else if resp.StatusCode/100 == 2 {
			cmd.port = trial.port
			return trial.typ, nil
		}
	}

	return featurebaseTypeUnknown, nil
}

func (cmd *Command) closeOutput() error {
	if cmd.output == nil {
		return nil
	}

	if closer, ok := cmd.output.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}

func (cmd *Command) handleLine(line string) error {
	// For single-line command handling, we handle either a meta-command, or
	// query parts, but not both. The logic is that any line which begins with
	// "\" will be handled as a meta-command, otherwise it will be handled as a
	// query.
	if len(line) == 0 {
		return nil
	} else if line[0] == byte('\\') {
		return cmd.handleLineAsMetaCommand(line)
	} else {
		return cmd.handleLineAsQueryParts(line)
	}
}

func (cmd *Command) handleLineAsMetaCommand(line string) error {
	_, mcs, err := cmd.splitter.split(line)
	if err != nil {
		return errors.Wrapf(err, "splitting line")
	}

	for i := range mcs {
		_, err := mcs[i].execute(cmd)
		if err != nil {
			return errors.Wrap(err, "executing meta command")
		}
	}

	return nil
}

func (cmd *Command) handleLineAsQueryParts(line string) error {
	qps, mcs, err := cmd.splitter.split(line)
	if err != nil {
		return errors.Wrapf(err, "splitting line")
	} else if len(mcs) > 0 {
		return errors.Errorf("--command does not support meta-commands")
	}

	// Add a termintor part to the end of []queryPart. We do this because the
	// command is coming in from the --command flag, it may not end with a
	// semi-colon, but we still want to execute it.
	if len(qps) > 0 {
		if _, ok := qps[len(qps)-1].(*partTerminator); !ok {
			qps = append(qps, newPartTerminator())
		}
	}

	for i := range qps {
		if qry, err := cmd.buffer.addPart(qps[i]); err != nil {
			return errors.Wrap(err, "adding part to buffer")
		} else if qry != nil {
			if err := cmd.executeAndWriteQuery(qry); err != nil {
				return errors.Wrap(err, "executing query")
			}
		}
	}
	return nil
}
