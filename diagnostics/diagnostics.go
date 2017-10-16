package diagnostics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sony/gobreaker"
)

// TODO: unique Cluster ID

// Default interval to sync diagnostics metrics.
const (
	DefaultDiagnosticsInterval = 1 * time.Hour
	DefaultVersionCheckURL     = "https://diagnostics.pilosa.com/v0/version"
)

type versionResponse struct {
	Version string `json:"version"`
	Message string `json:"message"`
}

// Diagnostics represents a client to the Pilosa cluster.
type Diagnostics struct {
	mu         sync.Mutex
	wg         sync.WaitGroup
	closing    chan struct{}
	host       string
	VersionURL string
	version    string
	startTime  int64
	start      time.Time

	metrics map[string]interface{}

	client   *http.Client
	interval time.Duration

	cb        *gobreaker.CircuitBreaker
	logOutput io.Writer
}

// New returns a pointer to a new Diagnostics Client given an addr in the format "hostname:port".
func New(host string) *Diagnostics {
	var st gobreaker.Settings
	st.Timeout = DefaultDiagnosticsInterval * 2

	return &Diagnostics{
		closing:    make(chan struct{}),
		host:       host,
		VersionURL: DefaultVersionCheckURL,
		startTime:  time.Now().Unix(),
		start:      time.Now(),
		client:     http.DefaultClient,
		metrics:    make(map[string]interface{}),
		interval:   DefaultDiagnosticsInterval,
		logOutput:  ioutil.Discard,
		cb:         gobreaker.NewCircuitBreaker(st),
	}
}

// SetVersion of locally running Pilosa Cluster to check against master.
func (d *Diagnostics) SetVersion(v string) {
	d.version = v
	d.Set("Version", v)
}

// schedule start the diagnostics service ticker.
func (d *Diagnostics) schedule() {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-d.closing:
			return
		case <-ticker.C:
			d.CheckVersion()
			d.Flush()
		}
	}
}

// Flush sends the current metrics.
func (d *Diagnostics) Flush() error {
	d.mu.Lock()
	d.metrics["uptime"] = (time.Now().Unix() - d.startTime)
	buf, _ := d.Encode()
	d.mu.Unlock()

	_, err := d.cb.Execute(func() (interface{}, error) {
		req, err := http.NewRequest("POST", d.host, bytes.NewReader(buf))
		req.Header.Set("Content-Type", "application/json")
		resp, err := d.client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		// TODO verify response
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return body, nil
	})

	return err
}

// Open starts the diagnostics metric go routine.
func (d *Diagnostics) Open() {
	d.wg.Add(1)
	go func() { defer d.wg.Done(); d.schedule() }()
}

// Close notify goroutine to stop.
func (d *Diagnostics) Close() error {
	close(d.closing)
	d.wg.Wait()
	return nil
}

// CheckVersion of the local build against Pilosa master.
func (d *Diagnostics) CheckVersion() error {
	var rsp versionResponse
	req, err := http.NewRequest("GET", d.VersionURL, nil)
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http: status=%d", resp.StatusCode)
	} else if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return fmt.Errorf("json decode: %s", err)
	}

	if err := d.CompareVersion(rsp.Version); err != nil {
		d.logger().Printf("%s\n", err.Error())
	}

	return nil
}

// CompareVersion check version strings.
func (d *Diagnostics) CompareVersion(value string) error {
	currentVersion := VersionSegments(value)
	localVersion := VersionSegments(d.version)

	if localVersion[0] < currentVersion[0] { //Major
		return fmt.Errorf("Warning: You are running an older version of Pilosa %s. The latest Major release is %s", d.version, value)
	} else if localVersion[1] < currentVersion[1] { // Minor
		return fmt.Errorf("Warning: You are running an older version of Pilosa %s. The latest Minor release is %s", d.version, value)
	} else if localVersion[2] < currentVersion[2] { // Patch
		return fmt.Errorf("There is a new patch relese of Pilosa availbale: %s", value)
	}

	return nil
}

// Encode metrics maps into the json message format
func (d *Diagnostics) Encode() ([]byte, error) {
	return json.Marshal(d.metrics)
}

// Set adds a key value metric.
func (d *Diagnostics) Set(name string, value interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.metrics[name] = value
}

// SetLogger Set the logger output type.
func (d *Diagnostics) SetLogger(logger io.Writer) {
	d.logOutput = logger
}

// logger returns a logger that writes to LogOutput.
func (d *Diagnostics) logger() *log.Logger {
	return log.New(d.logOutput, "", log.LstdFlags)
}

// VersionSegments returns the numeric segments of the version as a slice of ints.
func VersionSegments(segments string) []int {
	segments = strings.Trim(segments, "v")
	segments = strings.Split(segments, "-")[0]
	s := strings.Split(segments, ".")
	segmentSlice := make([]int, len(s))
	for i, v := range s {
		segmentSlice[i], _ = strconv.Atoi(v)
	}
	return segmentSlice
}
