package ctl

import (
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

	// commands holds the list of sql commands to be executed.
	commands []string
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
		Port:        "10101",
		HistoryPath: historyPath,
	}
}

func (cmd *CLICommand) Run(ctx context.Context) error {
	// Print the splash message.
	fmt.Print(splash)

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

	if !strings.HasPrefix(cmd.Host, "http") {
		cmd.Host = "http://" + cmd.Host
	}

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

func (cmd *CLICommand) executeCommands(ctx context.Context) error {
	// Clear out the buffered commands on any exit from this method.
	defer func() {
		cmd.commands = nil
	}()

	for _, sql := range cmd.commands {
		resp, err := http.Post(fmt.Sprintf("%s:%s/sql", cmd.Host, cmd.Port), "application/sql", strings.NewReader(sql))
		if err != nil {
			return errors.Wrapf(err, "posting query")
		}

		var sqlResponse response
		fullbod, err := io.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrap(err, "reading response")
		}
		err = json.Unmarshal(fullbod, &sqlResponse)
		if err != nil {
			fmt.Printf("couldn't decode response: %v\n", err)
			fmt.Printf("%s\n", fullbod)
		}

		err = sqlResponse.WriteOut(os.Stdout)
		if err != nil {
			return errors.Wrap(err, "writing out response")
		}
	}

	return nil
}

type response struct {
	Schema        featurebase.SQLSchema `json:"schema"`
	Data          [][]interface{}       `json:"data"`
	Error         string                `json:"error"`
	Warnings      []string              `json:"warnings"`
	ExecutionTime int64                 `json:"exec_time"`
}

func (r *response) WriteWarnings(w io.Writer) error {
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

func (r *response) WriteOut(w io.Writer) error {
	if r.Error != "" {
		if _, err := w.Write([]byte("Error: " + r.Error + "\n")); err != nil {
			return errors.Wrapf(err, "writing error: %s", r.Error)
		}
		return r.WriteWarnings(w)
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

	err := r.WriteWarnings(w)
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
