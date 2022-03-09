package ctl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
	featurebase "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/dax"
	queryerhttp "github.com/molecula/featurebase/v3/dax/queryer/http"
	"github.com/molecula/featurebase/v3/fbcloud"
	"github.com/pkg/errors"
)

const (
	promptBegin     string = "fbsql> "
	promptMid       string = "    -> "
	terminationChar string = ";"
	exitCommand     string = "exit"
	nullValue       string = "NULL"
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

	queryer FBQueryer
}

func NewCLICommand(stdin io.Reader, stdout, stderr io.Writer) *CLICommand {
	historyPath := ""
	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Printf("Error getting home directory, command history persistence will be disabled: %v\n", err)
	} else {
		historyDir := filepath.Join(home, ".featurebase")
		err := os.MkdirAll(historyDir, 0750)
		if err != nil {
			fmt.Printf("Creating directory for history: %v\n", err)
		} else {
			historyPath = filepath.Join(historyDir, "cli_history")
		}
	}
	return &CLICommand{
		Host:        "localhost",
		HistoryPath: historyPath,

		OrganizationID: "",
		DatabaseID:     "",
	}
}

// printQualifiers displays the currently set OrganizationID and DatabaseID.
func (cmd *CLICommand) printQualifiers() {
	fmt.Printf(" Host: %s\n  Org: %s\n   DB: %s\n",
		hostPort(cmd.Host, cmd.Port),
		cmd.OrganizationID,
		cmd.DatabaseID,
	)
}

func (cmd *CLICommand) setupClient() error {
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
		fmt.Println("Detected standard deployment")
		cmd.queryer = &standardQueryer{
			Host: cmd.Host,
			Port: cmd.Port,
		}
	case featurebaseTypeDAX:
		fmt.Println("Detected dax deployment")
		cmd.queryer = &daxQueryer{
			Host: cmd.Host,
			Port: cmd.Port,
		}
	case featurebaseTypeCloud:
		fmt.Println("Detected cloud deployment")
		cmd.queryer = &fbcloud.Queryer{
			Host: cmd.Host,

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

	for _, trial := range trials {
		url := hostPort(cmd.Host, trial.port) + trial.health
		if resp, err := http.Get(url); err != nil {
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
	fmt.Print(splash)
	err := cmd.setupClient()
	if err != nil {
		return errors.Wrap(err, "setting up client")
	}
	cmd.printQualifiers()

	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 promptBegin,
		HistoryFile:            cmd.HistoryPath,
		HistoryLimit:           100000,
		DisableAutoSaveHistory: true,
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
			fmt.Println()
		}

		// Read user provided input.
		line, err := rl.Readline()
		if err != nil {
			return errors.Wrap(err, "reading line")
		}

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
			partIsBlank := part == ""

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
			fmt.Printf("Couldn't save history: %v\n", err)
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
		return orig + " " + part
	}
}

type FBQueryer interface {
	Query(org, db, sql string) (*featurebase.SQLResponse, error)
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

		sqlResponse, err := cmd.queryer.Query(cmd.OrganizationID, cmd.DatabaseID, sql)
		if err != nil {
			fmt.Printf("making query: %v\n", err)
			continue
		}
		err = WriteOut(sqlResponse, os.Stdout)
		if err != nil {
			return errors.Wrap(err, "writing out response")
		}
	}

	return nil
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

func WriteWarnings(r *featurebase.SQLResponse, w io.Writer) error {
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

func WriteOut(r *featurebase.SQLResponse, w io.Writer) error {
	if r == nil {
		return errors.New("attempt to write out nil response")
	}
	if r.Error != "" {
		if _, err := w.Write([]byte("Error: " + r.Error + "\n")); err != nil {
			return errors.Wrapf(err, "writing error: %s", r.Error)
		}
		return WriteWarnings(r, w)
	}

	t := table.NewWriter()
	t.SetOutputMirror(w)

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

	err := WriteWarnings(r, w)
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

	if _, err := w.Write([]byte(fmt.Sprintf("\nExecution time: %dμs%s\n", r.ExecutionTime, lifeAffirmingMessage))); err != nil {
		return errors.Wrapf(err, "writing execution time: %s", r.Error)
	}

	return nil
}

func schemaToRow(schema featurebase.SQLSchema) []interface{} {
	ret := make([]interface{}, len(schema.Fields))
	for i, field := range schema.Fields {
		ret[i] = field.Name
	}
	return ret
}

// Ensure type implements interface.
var _ FBQueryer = (*standardQueryer)(nil)

// standardQueryer supports a standard featurebase deployment hitting the /sql
// endpoint with a payload containing only the sql statement.
type standardQueryer struct {
	Host string
	Port string
}

func (qryr *standardQueryer) Query(org, db, sql string) (*featurebase.SQLResponse, error) {
	buf := bytes.Buffer{}
	url := fmt.Sprintf("%s/sql", hostPort(qryr.Host, qryr.Port))

	buf.Write([]byte(sql))

	resp, err := http.Post(url, "application/json", &buf)
	if err != nil {
		return nil, errors.Wrapf(err, "posting query")
	}

	fullbod, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading response")
	}
	sqlResponse := &featurebase.SQLResponse{}
	if err := json.Unmarshal(fullbod, sqlResponse); err != nil {
		return nil, errors.Wrapf(err, "unmarshaling query response, body:\n'%s'\n", fullbod)
	}

	return sqlResponse, nil
}

// Ensure type implements interface.
var _ FBQueryer = (*daxQueryer)(nil)

// daxQueryer is similar to the standardQueryer except that it hits a different
// endpoint, and its payload is a json object which includes, in addition to the
// sql statement, things like org and db.
type daxQueryer struct {
	Host string
	Port string
}

func (qryr *daxQueryer) Query(org, db, sql string) (*featurebase.SQLResponse, error) {
	buf := bytes.Buffer{}
	url := fmt.Sprintf("%s/queryer/sql", hostPort(qryr.Host, qryr.Port))

	sqlReq := &queryerhttp.SQLRequest{
		OrganizationID: dax.OrganizationID(org),
		DatabaseID:     dax.DatabaseID(db),
		SQL:            sql,
	}
	if err := json.NewEncoder(&buf).Encode(sqlReq); err != nil {
		return nil, errors.Wrapf(err, "encoding sql request: %s", sql)
	}

	resp, err := http.Post(url, "application/json", &buf)
	if err != nil {
		return nil, errors.Wrapf(err, "posting query")
	}

	fullbod, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading response")
	}
	sqlResponse := &featurebase.SQLResponse{}
	if err := json.Unmarshal(fullbod, sqlResponse); err != nil {
		return nil, errors.Wrapf(err, "unmarshaling query response, body:\n'%s'\n", fullbod)
	}

	return sqlResponse, nil
}
