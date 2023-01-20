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
	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
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
	exitCommand     string = "exit"
	nullValue       string = "NULL"
)

var (
	Stdin  io.ReadCloser = os.Stdin
	Stdout io.Writer     = os.Stdout
	Stderr io.Writer     = os.Stderr
)

var (
	splash string = fmt.Sprintf(`FeatureBase CLI (%s)
Type "exit" to quit.
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

	// commands holds the list of sql commands to be executed.
	commands []string

	OrganizationID string `json:"org-id"`
	DatabaseID     string `json:"db-id"`

	Queryer Queryer `json:"-"`

	Stdin  io.ReadCloser `json:"-"`
	Stdout io.Writer     `json:"-"`
	Stderr io.Writer     `json:"-"`
}

func NewCLICommand(logdest logger.Logger) *CLICommand {
	return &CLICommand{
		Host:        defaultHost,
		HistoryPath: "",

		OrganizationID: "",
		DatabaseID:     "",

		Stdin:  Stdin,
		Stdout: Stdout,
		Stderr: Stderr,
	}
}

func (cmd *CLICommand) setupHistory() {
	// If HistoryPath has already been configured (i.e. with a command flag),
	// don't bother setting up the default in the home directory.
	if cmd.HistoryPath != "" {
		return
	}

	historyPath := ""
	if home, err := os.UserHomeDir(); err != nil {
		cmd.Printf("Error getting home directory, command history persistence will be disabled: %v\n", err)
	} else {
		historyDir := filepath.Join(home, ".featurebase")
		err := os.MkdirAll(historyDir, 0o750)
		if err != nil {
			cmd.Printf("Creating directory for history: %v\n", err)
		} else {
			historyPath = filepath.Join(historyDir, "cli_history")
		}
	}
	cmd.HistoryPath = historyPath
}

// printQualifiers displays the currently set OrganizationID and DatabaseID.
func (cmd *CLICommand) printQualifiers() {
	cmd.Printf(" Host: %s\n  Org: %s\n   DB: %s\n",
		hostPort(cmd.Host, cmd.Port),
		cmd.OrganizationID,
		cmd.DatabaseID,
	)
}

func (cmd *CLICommand) setupClient() error {
	// If the Queryer has already been set (in tests for example), don't bother
	// trying to detect it.
	if cmd.Queryer != nil {
		return nil
	}

	if strings.TrimSpace(cmd.Host) == "" {
		return errors.Errorf("no host provided")
	}

	if !strings.HasPrefix(cmd.Host, "http") {
		cmd.Host = "http://" + cmd.Host
	}

	typ, err := cmd.detectFBType()
	if err != nil {
		return errors.Wrap(err, "detecting FeatureBase deployment type")
	}

	switch typ {
	case featurebaseTypeStandard:
		cmd.Printf("Detected standard deployment\n")
		cmd.Queryer = &standardQueryer{
			Host: cmd.Host,
			Port: cmd.Port,
		}
	case featurebaseTypeDAX:
		cmd.Printf("Detected dax deployment\n")
		cmd.Queryer = &daxQueryer{
			Host: cmd.Host,
			Port: cmd.Port,
		}
	case featurebaseTypeCloud:
		cmd.Printf("Detected cloud deployment\n")
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
	featurebaseTypeStandard featurebaseType = "standard"
	featurebaseTypeDAX      featurebaseType = "dax"
	featurebaseTypeCloud    featurebaseType = "cloud"
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

	if cmd.Port != "" {
		trials = append(trials,
			// dax
			trial{
				port:   cmd.Port,
				health: "/queryer/health",
				typ:    featurebaseTypeDAX,
			},
			// standard
			trial{
				port:   cmd.Port,
				health: "/status",
				typ:    featurebaseTypeStandard,
			},
		)
	} else if strings.HasPrefix(cmd.Host, "https") {
		// https suggesting we might be connecting to a cloud host
		trials = append(trials,
			// cloud
			trial{
				port:   "",
				health: "health",
				typ:    featurebaseTypeCloud,
			},
		)
	} else {
		// Try default ports just in case.
		trials = append(trials,
			// dax
			trial{
				port:   "8080",
				health: "/queryer/health",
				typ:    featurebaseTypeDAX,
			},
			// standard
			trial{
				port:   "10101",
				health: "/status",
				typ:    featurebaseTypeStandard,
			},
		)
	}

	client := http.Client{
		Timeout: 100 * time.Millisecond,
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

	return featurebaseTypeCloud, nil
}

func (cmd *CLICommand) Run(ctx context.Context) error {
	// Print the splash message.
	cmd.Printf(splash)
	cmd.setupHistory()
	if err := cmd.setupClient(); err != nil {
		return errors.Wrap(err, "setting up client")
	}
	cmd.printQualifiers()

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

	// partialCommand holds all input prior to receiving a termination
	// character.
	var partialCommand string

	// inMidCommand indicates whether a partial command has been received and
	// we're still waiting for a termination character.
	var inMidCommand bool

	for {
		if inMidCommand {
			rl.SetPrompt(promptMid)
		} else {
			rl.SetPrompt(promptBegin)
			// Add some white space before each new prompt.
			cmd.Printf("\n")
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
		line += "\n"

		if !inMidCommand {
			// Handle the exit command.
			if line == exitCommand || line == exitCommand+terminationChar {
				break
			}
		}

		// Look for a termination character;
		parts := strings.Split(line, terminationChar)

		// Length of 1 means a termination character was not received.
		if len(parts) == 1 {
			if parts[0] != "" {
				partialCommand = appendCommand(partialCommand, parts[0])
				inMidCommand = true
			}
			continue
		}

		for i, part := range parts {
			partIsFinal := i == len(parts)-1
			partIsBlank := strings.TrimSpace(part) == ""

			if partIsBlank && partIsFinal {
				continue
			}

			if partIsBlank && !partIsFinal {
				if inMidCommand {
					cmd.commands = append(cmd.commands, strings.TrimSpace(partialCommand))
					partialCommand = ""
					inMidCommand = false
				}
				continue
			}

			if !partIsBlank && partIsFinal {
				partialCommand = part
				inMidCommand = true
				continue
			}

			if !partIsBlank && !partIsFinal {
				partialCommand = appendCommand(partialCommand, part)
				cmd.commands = append(cmd.commands, strings.TrimSpace(partialCommand))
				partialCommand = ""
				inMidCommand = false
			}
		}

		err = rl.SaveHistory(strings.Join(cmd.commands, "; ") + ";")
		if err != nil {
			cmd.Printf("Couldn't save history: %v\n", err)
		}

		if err := cmd.executeCommands(ctx); err != nil {
			return errors.Wrap(err, "executing commands")
		}
	}

	return nil
}

func appendCommand(orig string, part string) string {
	if orig == "" {
		return part
	} else {
		return orig + part
	}
}

func (cmd *CLICommand) executeCommands(ctx context.Context) error {
	// Clear out the buffered commands on any exit from this method.
	defer func() {
		cmd.commands = nil
	}()

	for _, sql := range cmd.commands {
		// Handle non-sql commands (for example, SET commands).
		if handled, err := cmd.handleIfNonSQLCommand(ctx, sql); err != nil {
			return errors.Wrapf(err, "handling non-SQL command: %s", sql)
		} else if handled {
			continue
		}

		sqlResponse, err := cmd.Queryer.Query(cmd.OrganizationID, cmd.DatabaseID, sql)
		if err != nil {
			cmd.Printf("making query: %v\n", err)
			continue
		}
		err = writeOut(sqlResponse, cmd.Stdout, cmd.Stderr)
		if err != nil {
			return errors.Wrap(err, "writing out response")
		}
	}

	return nil
}

// Printf is a helper method which sends the given payload to stdout.
func (cmd *CLICommand) Printf(format string, a ...any) {
	out := fmt.Sprintf(format, a...)
	cmd.Stdout.Write([]byte(out))
}

// handleIfNonSQLCommand will handle special case command like "SET ..." and
// "USE ...". If the sql command matches one of these conditions and is handled,
// the bool returned will be true;
func (cmd *CLICommand) handleIfNonSQLCommand(ctx context.Context, sql string) (bool, error) {
	var handled bool

	// Get the first token from the SQL:
	parts := strings.Split(sql, " ")
	if len(parts) < 1 {
		return handled, nil
	}

	token := strings.ToUpper(parts[0])

	// Supported:
	// SET ORG acme
	// SET DB db1
	// USE db1
	switch token {
	case "SET":
		handled = true
		switch len(parts) {
		case 1:
			// This will fall through and just print the qualifiers.
		case 3:
			switch strings.ToUpper(parts[1]) {
			case "HOST":
				cmd.Host = parts[2]
			case "ORG":
				cmd.OrganizationID = parts[2]
			case "DB":
				cmd.DatabaseID = parts[2]
			}
		default:
			return handled, errors.Errorf("SET command takes a name and a value (SET DB db1)")
		}

	case "USE":
		handled = true
		if len(parts) != 2 {
			return handled, errors.Errorf("USE command takes a single value (USE db1)")
		}
		cmd.DatabaseID = parts[1]
	default:
		return handled, nil
	}

	cmd.printQualifiers()
	return handled, nil
}

func writeWarnings(r *featurebase.WireQueryResponse, w io.Writer) error {
	if len(r.Warnings) > 0 {
		if _, err := w.Write([]byte("\n")); err != nil {
			return errors.Wrapf(err, "writing warning: %s", r.Error)
		}
		for _, warning := range r.Warnings {
			if _, err := w.Write([]byte("Warning: " + warning + "\n")); err != nil {
				return errors.Wrapf(err, "writing warning: %s", r.Error)
			}
		}
	}
	return nil
}

func writeOut(r *featurebase.WireQueryResponse, wOut io.Writer, wErr io.Writer) error {
	if r == nil {
		return errors.New("attempt to write out nil response")
	}
	if r.Error != "" {
		if _, err := wErr.Write([]byte("Error: " + r.Error + "\n")); err != nil {
			return errors.Wrapf(err, "writing error: %s", r.Error)
		}
		return writeWarnings(r, wOut)
	}

	t := table.NewWriter()
	t.SetOutputMirror(wOut)

	// Don't uppercase the header values.
	t.Style().Format.Header = text.FormatDefault

	t.AppendHeader(schemaToRow(r.Schema))
	for _, row := range r.Data {
		// If the value is nil, replace it with a null string; go-pretty doesn't
		// expect nil pointers in the data values.
		for i := range row {
			if row[i] == nil {
				row[i] = nullValue
			}
		}
		t.AppendRow(table.Row(row))
	}
	t.Render()

	err := writeWarnings(r, wOut)
	if err != nil {
		return err
	}
	lifeAffirmingMessage := ""
	if r.ExecutionTime < 1000000 {
		lifeAffirmingMessage = " (You're welcome! 🚀)"
	}

	if r.ExecutionTime > 5000000 {
		lifeAffirmingMessage = " (Sorry! That took longer than expected 😭)"
	}

	if _, err := wOut.Write([]byte(fmt.Sprintf("\nExecution time: %dμs%s\n", r.ExecutionTime, lifeAffirmingMessage))); err != nil {
		return errors.Wrapf(err, "writing execution time: %s", r.Error)
	}

	return nil
}

func schemaToRow(schema featurebase.WireQuerySchema) []interface{} {
	ret := make([]interface{}, len(schema.Fields))
	for i, field := range schema.Fields {
		ret[i] = field.Name
	}
	return ret
}
