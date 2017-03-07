package ctl

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/pilosa/pilosa"
)

// SortCommand represents a command for sorting import data.
type SortCommand struct {
	// Filename to sort
	Path string

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewSortCommand returns a new instance of SortCommand.
func NewSortCommand(stdin io.Reader, stdout, stderr io.Writer) *SortCommand {
	return &SortCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// Run executes the sort command.
func (cmd *SortCommand) Run(ctx context.Context) error {
	// Open file for reading.
	f, err := os.Open(cmd.Path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Read rows as bits.
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	a := make([]pilosa.Bit, 0, 1000000)
	for {
		bitmapID, profileID, timestamp, err := readCSVRow(r)
		if err == io.EOF {
			break
		} else if err == errBlank {
			continue
		} else if err != nil {
			return err
		}
		a = append(a, pilosa.Bit{BitmapID: bitmapID, ProfileID: profileID, Timestamp: timestamp})
	}

	// Sort bits by position.
	sort.Sort(pilosa.BitsByPos(a))

	// Rewrite to STDOUT.
	w := bufio.NewWriter(cmd.Stdout)
	buf := make([]byte, 0, 1024)
	for _, bit := range a {
		// Write CSV to buffer.
		buf = buf[:0]
		buf = strconv.AppendUint(buf, bit.BitmapID, 10)

		buf = append(buf, ',')
		buf = strconv.AppendUint(buf, bit.ProfileID, 10)

		if bit.Timestamp != 0 {
			buf = append(buf, ',')
			buf = append(buf, time.Unix(0, bit.Timestamp).UTC().Format(pilosa.TimeFormat)...)
		}

		buf = append(buf, '\n')

		// Write to output.
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}

	// Ensure buffer is flushed before exiting.
	if err := w.Flush(); err != nil {
		return err
	}

	return nil
}

// readCSVRow reads a bitmap/profile pair from a CSV row.
func readCSVRow(r *csv.Reader) (bitmapID, profileID uint64, timestamp int64, err error) {
	// Read CSV row.
	record, err := r.Read()
	if err != nil {
		return 0, 0, 0, err
	}

	// Ignore blank rows.
	if record[0] == "" {
		return 0, 0, 0, errBlank
	} else if len(record) < 2 {
		return 0, 0, 0, fmt.Errorf("bad column count: %d", len(record))
	}

	// Parse bitmap id.
	bitmapID, err = strconv.ParseUint(record[0], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid bitmap id: %q", record[0])
	}

	// Parse bitmap id.
	profileID, err = strconv.ParseUint(record[1], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid profile id: %q", record[1])
	}

	// Parse timestamp, if available.
	if len(record) > 2 && record[2] != "" {
		t, err := time.Parse(pilosa.TimeFormat, record[2])
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalid timestamp: %q", record[2])
		}
		timestamp = t.UnixNano()
	}

	return bitmapID, profileID, timestamp, nil
}

// errBlank indicates a blank row in a CSV file.
var errBlank = errors.New("blank row")
