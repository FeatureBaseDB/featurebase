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
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
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

type CLICommand struct {
	Host        string `json:"host"`
	Port        string `json:"port"`
	HistoryPath string `json:"history-path"`

	// Cloud Auth
	ClientID string `json:"client-id"`
	Region   string `json:"region"`
	Email    string `json:"email"`
	Password string `json:"password"`

	splitter   *splitter
	buffer     *buffer
	workingDir *workingDir

	OrganizationID string `json:"org-id"`
	Database       string `json:"db"`
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

	// quit gets closed when Run should stop listening for input.
	quit chan struct{}
}

func NewCLICommand(logdest logger.Logger) *CLICommand {
	return &CLICommand{
		Host:        defaultHost,
		HistoryPath: "",

		OrganizationID: "",
		Database:       "",

		splitter:   newSplitter(),
		buffer:     newBuffer(),
		workingDir: newWorkingDir(),

		Stdin:  Stdin,
		Stdout: Stdout,
		Stderr: Stderr,

		output:       Stdout,
		writeOptions: defaultWriteOptions(),

		quit: make(chan struct{}),
	}
}

// Run is the main entry-point to the CLI. Currently it handles the interaction
// with a user, as opposed to calling `featurebase cli` in a script.
func (cmd *CLICommand) Run(ctx context.Context) error {
	// Print the splash message.
	cmd.Printf(splash)
	cmd.setupHistory()
	if err := cmd.setupClient(); err != nil {
		return errors.Wrap(err, "setting up client")
	}
	cmd.printConnInfo()
	if err := cmd.connectToDatabase(cmd.Database); err != nil {
		cmd.Errorf(errors.Wrap(err, "connecting to database").Error() + "\n")
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 promptBegin,
		HistoryFile:            cmd.HistoryPath,
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
		if err != nil {
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
func (cmd *CLICommand) close() error {
	return cmd.closeOutput()
}

func (cmd *CLICommand) executeAndWriteQuery(qry query) error {
	queryResponse, err := cmd.executeQuery(qry)
	if err != nil {
		return errors.Wrap(err, "making query")
	}

	if err := writeTable(queryResponse, cmd.writeOptions, cmd.output, cmd.Stdout, cmd.Stderr); err != nil {
		return errors.Wrap(err, "writing out response")
	}

	return nil
}

func (cmd *CLICommand) executeQuery(qry query) (*featurebase.WireQueryResponse, error) {
	return cmd.Queryer.Query(cmd.OrganizationID, cmd.databaseID, qry.Reader())
}

// Printf is a helper method which sends the given payload to stdout.
func (cmd *CLICommand) Printf(format string, a ...any) {
	out := fmt.Sprintf(format, a...)
	cmd.Stdout.Write([]byte(out))
}

// Outputf is a helper method which sends the given payload to output.
func (cmd *CLICommand) Outputf(format string, a ...any) {
	out := fmt.Sprintf(format, a...)
	cmd.output.Write([]byte(out))
}

// Errorf is a helper method which sends the given payload to stderr.
func (cmd *CLICommand) Errorf(format string, a ...any) {
	out := fmt.Sprintf(format, a...)
	cmd.Stderr.Write([]byte(out))
}

func (cmd *CLICommand) setupHistory() {
	// If HistoryPath has already been configured (i.e. with a command flag),
	// don't bother setting up the default in the home directory.
	if cmd.HistoryPath != "" {
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
	cmd.HistoryPath = historyPath
}

// printConnInfo displays the currently set host.
// TODO(tlt): extend this to be the output of the /conninfo meta-command.
func (cmd *CLICommand) printConnInfo() {
	cmd.Printf("Host: %s\n", hostPort(cmd.Host, cmd.Port))
}

func (cmd *CLICommand) connectToDatabase(dbName string) error {
	if dbName == "" {
		cmd.databaseID = ""
		cmd.databaseName = ""
		cmd.Printf(cmd.connectionMessage())
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
			cmd.Printf(cmd.connectionMessage())
			return nil
		}
	}
	return errors.Errorf("invalid database: %s", dbName)
}

func (cmd *CLICommand) orgMessage() string {
	if cmd.OrganizationID == "" {
		return "You have not set an organization.\n"
	}
	return fmt.Sprintf("You have set organization \"%s\".\n", cmd.OrganizationID)
}

func (cmd *CLICommand) connectionMessage() string {
	if cmd.databaseName == "" {
		return "You are not connected to a database.\n"
	}
	return fmt.Sprintf("You are now connected to database \"%s\" (%s) as user \"???\".\n", cmd.databaseName, cmd.databaseID)
}

func (cmd *CLICommand) setupClient() error {
	// If the Queryer has already been set (in tests for example), don't bother
	// trying to detect it.
	if cmd.Queryer != nil {
		return nil
	}

	if strings.TrimSpace(cmd.Host) == "" {
		return errors.Errorf("no host provided\n")
	}

	if !strings.HasPrefix(cmd.Host, "http") {
		cmd.Host = "http://" + cmd.Host
	}

	typ, err := cmd.detectFBType()
	if err != nil {
		return errors.Wrap(err, "detecting FeatureBase deployment type")
	}

	switch typ {
	case featurebaseTypeOnPremClassic:
		cmd.Printf("Detected on-prem, classic deployment.\n")
		cmd.Queryer = &standardQueryer{
			Host: cmd.Host,
			Port: cmd.Port,
		}
	case featurebaseTypeOnPremServerless:
		cmd.Printf("Detected on-prem, serverless deployment.\n")
		cmd.Queryer = &serverlessQueryer{
			Host: cmd.Host,
			Port: cmd.Port,
		}
	case featurebaseTypeCloud:
		cmd.Printf("Detected cloud deployment.\n")
		cmd.Queryer = &fbcloud.Queryer{
			Host: hostPort(cmd.Host, cmd.Port),

			ClientID: cmd.ClientID,
			Region:   cmd.Region,
			Email:    cmd.Email,
			Password: cmd.Password,
		}
	case featurebaseTypeUnknown:
		cmd.Printf("Could not detect deployment\n")
		// cmd.Queryer = &nopQueryer{}
		// Instead of using a no-op queryer when the type can't be detected, we
		// default to using a cloud queryer.
		cmd.Queryer = &fbcloud.Queryer{
			Host: hostPort(cmd.Host, cmd.Port),

			ClientID: cmd.ClientID,
			Region:   cmd.Region,
			Email:    cmd.Email,
			Password: cmd.Password,
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
func (cmd *CLICommand) detectFBType() (featurebaseType, error) {
	type trial struct {
		port   string
		health string
		typ    featurebaseType
	}

	// trials is populated with the url/endpoints to try in order to detect if a
	// process is running there which can support the cli requests.
	trials := []trial{}

	var clientTimeout time.Duration
	if cmd.Port != "" {
		clientTimeout = 100 * time.Millisecond
		trials = append(trials,
			// on-prem, serverless
			trial{
				port:   cmd.Port,
				health: "/queryer/health",
				typ:    featurebaseTypeOnPremServerless,
			},
			// on-prem, classic
			trial{
				port:   cmd.Port,
				health: "/status",
				typ:    featurebaseTypeOnPremClassic,
			},
		)
	} else if strings.HasPrefix(cmd.Host, "https") {
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
		url := hostPort(cmd.Host, trial.port) + trial.health
		if resp, err := client.Get(url); err != nil {
			continue
		} else if resp.StatusCode/100 == 2 {
			cmd.Port = trial.port
			return trial.typ, nil
		}
	}

	return featurebaseTypeUnknown, nil
}

func (cmd *CLICommand) closeOutput() error {
	if cmd.output == nil {
		return nil
	}

	if closer, ok := cmd.output.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}
