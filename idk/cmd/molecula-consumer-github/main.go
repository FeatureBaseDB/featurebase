package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"time"

	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/jaffee/commandeer/pflag"
	"github.com/pkg/errors"
)

const (
	MinWaitDuration = 5 * time.Minute
)

var (
	ghEventsVar       = expvar.NewInt("gh_events")
	ghEventsPerSecVar = expvar.NewFloat("gh_events_per_sec")
)

func main() {
	m := NewMain()
	if err := pflag.LoadEnv(m, "IDKGITHUB_", nil); err != nil {
		log.Fatal(err)
	}
	m.Rename()
	if m.DryRun {
		log.Printf("%+v\n", m)
		return
	}
	if err := m.Run(); err != nil {
		log := m.Log()
		if log == nil {
			// if we fail before a logger was instantiated
			logger.NewStandardLogger(os.Stderr).Errorf("Error running command: %v", err)
			os.Exit(1)
		}
		log.Errorf("Error running command: %v", err)
		os.Exit(1)
	}
}

type Main struct {
	idk.Main   `flag:"!embed"`
	RecordType string    `help:"Output record type"`
	StartTime  time.Time `help:"Start time"`
	EndTime    time.Time `help:"End time, optional"`
	CacheDir   string    `help:"Directory for local data files"`
}

func NewMain() *Main {
	m := &Main{
		Main:      *idk.NewMain(),
		StartTime: time.Now().UTC().Truncate(24 * time.Hour),
	}
	m.Main.Namespace = "ingester_github"

	// Start streaming events to channel in timestamp order.

	m.NewSource = func() (idk.Source, error) {
		if !IsValidRecordType(m.RecordType) {
			return nil, errors.New("invalid record type; must be 'event', 'user', 'issue' or 'repo'")
		}
		events := make(chan Event)
		go m.stream(events)
		source := NewSource(events)
		source.RecordType = m.RecordType
		source.Log = m.Main.Log()
		// source.JustDoIt = m.JustDoIt
		return source, nil
	}
	return m
}

func (m *Main) Run() error {
	// Set up HTTP endpoint to provide /debug/vars & /debug/pprof
	fmt.Println("Serving debug endpoint at http://localhost:7070/debug")
	go func() { _ = http.ListenAndServe(":7070", nil) }()

	go m.monitor()

	return m.Main.Run()
}

func (m *Main) stream(events chan Event) {
	defer close(events)

	// Ensure start time is aligned to hour.
	t := m.StartTime.Truncate(1 * time.Hour)

	for {
		// Stop once we reach the end time.
		if !m.EndTime.IsZero() && t.After(m.EndTime) {
			m.Main.Log().Printf("end of time range, stopping")
			break
		}

		// Fetch JSON data and convert to CSV.
		// If the data doesn't exist, wait and retry.
		if err := m.process(events, t); err == errNotFound {
			d := time.Until(t)
			if d < MinWaitDuration {
				d = MinWaitDuration
			}
			m.Main.Log().Printf("cannot fetch data for %s, waiting %s", t.Format(time.RFC3339), d)
			time.Sleep(d)
			continue
		} else if err != nil {
			m.Main.Log().Printf("error processing data: %s", err)
			events <- Event{err: err}
			return
		}

		// Move to the next timestamp.
		t = t.Add(time.Hour)
	}
}

// process fetches the GitHub Archive gzipped JSON data streams each event to the events channel.
func (m *Main) process(events chan Event, t time.Time) error {
	log.Printf("processing %s", t.Format(time.RFC3339))
	defer log.Printf("done processing %s", t.Format(time.RFC3339))

	// Fetch a reader for the timestamp.
	r, err := m.openURLReader(t)
	if err != nil {
		return err
	}
	defer r.Close()

	// Decompress stream.
	gr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gr.Close()

	// Read data as JSON.
	dec := json.NewDecoder(gr)
	counter := 0
	for {
		// Unmarshal single event from JSON stream.
		var event Event
		if err := dec.Decode(&event); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		if counter%20000 == 0 {
			m.Main.Log().Printf("record: id=%d", event.ID)
		}
		counter++

		// Send event to channel.
		events <- event
		ghEventsVar.Add(1) // total event count
	}
}

// monitor runs in a separate goroutine and updates the ingestion rate stats.
func (m *Main) monitor() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastTime time.Time
	var lastN int64
	for range ticker.C {
		now, n := time.Now(), ghEventsVar.Value()
		if !lastTime.IsZero() {
			ghEventsPerSecVar.Set(float64(lastN-n) / lastTime.Sub(now).Seconds())
		}
		lastTime, lastN = now, n
	}
}

type Source struct {
	RecordType string
	Log        logger.Logger

	events chan Event
}

func NewSource(events chan Event) *Source {
	return &Source{events: events}
}

func (s *Source) Record() (idk.Record, error) {
	for {
		event, ok := <-s.events
		if !ok {
			return nil, io.EOF
		}
		// Break out the error to hand back to IDK.
		if event.err != nil {
			return nil, event.err
		}

		var record idk.Record
		switch s.RecordType {
		case RecordTypeEvent:
			record = EventRecord(event)
		case RecordTypeUser:
			record = UserRecord(event)
		case RecordTypeRepo:
			record = RepoRecord(event)
		case RecordTypeIssue:
			record = IssueRecord(event)
		default:
			panic(fmt.Errorf("invalid source record type: %q", s.RecordType))
		}

		// If record implementation a validation function then check it.
		// Retry the next record if the record is invalid.
		if record, ok := record.(interface {
			Valid() bool
		}); ok && !record.Valid() {
			continue
		}
		return record, nil
	}
}

func (s *Source) Schema() []idk.Field {
	switch s.RecordType {
	case RecordTypeEvent:
		return s.eventSchema()
	case RecordTypeUser:
		return s.userSchema()
	case RecordTypeRepo:
		return s.repoSchema()
	case RecordTypeIssue:
		return s.issueSchema()
	default:
		panic(fmt.Sprintf("no schema for record type: %q", s.RecordType))
	}
}

func (s *Source) eventSchema() []idk.Field {
	idMin := int64(0)
	idMax := int64(1 << 31)
	return []idk.Field{
		idk.IDField{NameVal: "id"},
		idk.StringField{NameVal: "type"},
		idk.IDField{NameVal: "actor_id", Quantum: "YMDH"},
		idk.IDField{NameVal: "repo_id", Quantum: "YMDH"},
		idk.IntField{NameVal: "actor_bsi", Min: &idMin, Max: &idMax},
		idk.IntField{NameVal: "repo_bsi", Min: &idMin, Max: &idMax},
		idk.RecordTimeField{NameVal: "created_at", Layout: time.RFC3339},
	}
}

func (s *Source) userSchema() []idk.Field {
	return []idk.Field{
		idk.StringField{NameVal: "login"},
		idk.IDField{NameVal: "github_id"},
	}
}

func (s *Source) repoSchema() []idk.Field {
	return []idk.Field{
		idk.StringField{NameVal: "name"},
		idk.IDField{NameVal: "github_id"},
	}
}

func (s *Source) issueSchema() []idk.Field {
	idMin := int64(0)
	idMax := int64(1 << 31)
	return []idk.Field{
		idk.IDField{NameVal: "id"},
		idk.StringField{NameVal: "url"},
		idk.IntField{NameVal: "number", Min: &idMin, Max: &idMax},
		idk.IDField{NameVal: "comments", Mutex: true},
		idk.RecordTimeField{NameVal: "created_at", Layout: time.RFC3339},
	}
}

func (s *Source) Close() error {
	return nil
}

// Output type constants.
const (
	RecordTypeEvent = "event"
	RecordTypeUser  = "user"
	RecordTypeRepo  = "repo"
	RecordTypeIssue = "issue"
)

// IsValidRecordType returns true if s is a valid record type.
func IsValidRecordType(s string) bool {
	switch s {
	case RecordTypeEvent, RecordTypeUser, RecordTypeRepo, RecordTypeIssue:
		return true
	default:
		return false
	}
}

type EventRecord Event

func (r EventRecord) Data() []interface{} {
	return []interface{}{r.ID, r.Type, r.Actor.ID, r.Repo.ID, r.Actor.ID, r.Repo.ID, r.CreatedAt}
}

func (r EventRecord) Commit(ctx context.Context) error { return nil }

type UserRecord Event

func (r UserRecord) Data() []interface{} {
	return []interface{}{r.Actor.Login, r.Actor.ID}
}

func (r UserRecord) Commit(ctx context.Context) error { return nil }

type RepoRecord Event

func (r RepoRecord) Data() []interface{} {
	return []interface{}{r.Repo.Name, r.Repo.ID}
}

func (r RepoRecord) Commit(ctx context.Context) error { return nil }

type IssueRecord Event

func (r IssueRecord) Valid() bool {
	return r.Type == "IssuesEvent" || r.Type == "IssueCommentEvent"
}

func (r IssueRecord) Data() []interface{} {
	var issue Issue
	switch r.Type {
	case "IssuesEvent":
		var payload IssuePayload
		_ = json.Unmarshal(r.Payload, &payload)
		issue = payload.Issue
	case "IssueCommentEvent":
		var payload IssueCommentPayload
		_ = json.Unmarshal(r.Payload, &payload)
		issue = payload.Issue
	}
	return []interface{}{issue.ID, issue.URL, issue.Number, issue.Comments, r.CreatedAt}
}

func (r IssueRecord) Commit(ctx context.Context) error { return nil }

func (m *Main) openURLReader(t time.Time) (io.ReadCloser, error) {
	filename := fmt.Sprintf("%04d-%02d-%02d-%d.json.gz", t.Year(), t.Month(), t.Day(), t.Hour())
	cachePath := filepath.Join(m.CacheDir, filename)

	if m.CacheDir != "" {
		if fi, err := os.Stat(cachePath); err == nil && fi.Size() != 0 {
			m.Log().Printf("using cached file")
			return os.Open(cachePath)
		}
	}

	rawurl := "https://data.gharchive.org/" + filename
	resp, err := http.Get(rawurl)
	if err != nil {
		return nil, err
	} else if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, errNotFound
	} else if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, fmt.Errorf("invalid status code: code=%d url=%s", resp.StatusCode, rawurl)
	}

	// If no cache directory, return body directly.
	if m.CacheDir == "" {
		return resp.Body, nil
	}

	// If cache enabled, write to file first and then return.
	if buf, err := io.ReadAll(resp.Body); err != nil {
		return nil, err
	} else if err := os.WriteFile(cachePath+".tmp", buf, 0666); err != nil {
		return nil, err
	} else if err := os.Rename(cachePath+".tmp", cachePath); err != nil {
		return nil, err
	}
	m.Log().Printf("cached file written")
	return os.Open(cachePath)
}

type Event struct {
	ID        int             `json:"id,string"`
	Type      string          `json:"type"`
	Actor     *Actor          `json:"actor"`
	Repo      *Repo           `json:"repo"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`

	err error
}

type Actor struct {
	ID    int    `json:"id"`
	Login string `json:"login"`
}

type Repo struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type Issue struct {
	ID       int    `json:"id"`
	URL      string `json:"url"`
	Number   int    `json:"number"`
	Comments int    `json:"comments"`
}

type IssuePayload struct {
	Issue Issue `json:"issue"`
}

type IssueCommentPayload struct {
	Issue Issue `json:"issue"`
}

// errNotFound is returned when a data file does not exist.
var errNotFound = errors.New("not found")
