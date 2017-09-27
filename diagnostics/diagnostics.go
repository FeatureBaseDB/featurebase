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

	"github.com/pilosa/pilosa"
)

// TODO: white list of statsd metrics to use
// TODO: unique Cluster ID
// TODO: how should this be disabled, config

// Default interval to sync diagnostics metrics.
const (
	DefaultDiagnosticsInterval = 10 * time.Second
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

	counts  map[string]int64
	metrics map[string]string

	client   *http.Client
	interval time.Duration

	logOutput io.Writer
}

// New returns a pointer to a new Diagnostics Client given an addr in the format "hostname:port".
func New(host string) *Diagnostics {
	return &Diagnostics{
		closing:    make(chan struct{}),
		host:       host,
		VersionURL: DefaultVersionCheckURL,
		startTime:  time.Now().Unix(),
		start:      time.Now(),
		client:     http.DefaultClient,
		counts:     make(map[string]int64),
		metrics:    make(map[string]string),
		interval:   DefaultDiagnosticsInterval,
		logOutput:  ioutil.Discard,
	}
}

// SetVersion of locally running Pilosa Cluster to check against master.
func (d *Diagnostics) SetVersion(v string) {
	d.version = v
	d.Set("Version", v, 1.0)
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
	d.metrics["uptime"] = strconv.FormatInt((time.Now().Unix() - d.startTime), 10)

	buf, _ := d.MarshalJSON()
	d.Reset()
	d.mu.Unlock()

	// d.logger().Println(string(buf))
	req, err := http.NewRequest("POST", d.host, bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// TODO verify response
	// Read response into buffer.
	// body, err := ioutil.ReadAll(resp.Body)
	// if err != nil {
	// 	return err
	// }

	// TODO circuit breaker
	return nil
}

// Reset clears the incremented metrics.
func (d *Diagnostics) Reset() {
	d.counts = make(map[string]int64)
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

// MarshalJSON custom marshall string and int maps together.
func (d *Diagnostics) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	length := len(d.counts)
	count := 0

	for key, value := range d.counts {
		jsonValue, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		buffer.WriteString(fmt.Sprintf("\"%s\":%s", key, string(jsonValue)))
		count++
		if count < length {
			buffer.WriteString(",")
		}
	}
	if length > 0 {
		buffer.WriteString(",")
	}
	length = len(d.metrics)
	count = 0
	for key, value := range d.metrics {
		jsonValue, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		buffer.WriteString(fmt.Sprintf("\"%s\":%s", key, string(jsonValue)))
		count++
		if count < length {
			buffer.WriteString(",")
		}
	}

	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

// Stats interface implementation.

// Tags no-op.
func (d *Diagnostics) Tags() []string {
	return nil
}

// WithTags no-op.
func (d *Diagnostics) WithTags(tags ...string) pilosa.StatsClient {
	return d
}

// Count tracks the number of times something occurs per diagnostic period.
func (d *Diagnostics) Count(name string, value int64, rate float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.counts[name] += value
}

// CountWithCustomTags Tracks the number of times something occurs per diagnostic period.
func (d *Diagnostics) CountWithCustomTags(name string, value int64, rate float64, tags []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.counts[name] += value
}

// Gauge records the value of a metric.
func (d *Diagnostics) Gauge(name string, value float64, rate float64) {
	d.Set(name, strconv.FormatFloat(value, 'f', -1, 64), rate)
}

// Histogram is a no-op.
func (d *Diagnostics) Histogram(name string, value float64, rate float64) {
}

// Set adds a key value metric.
func (d *Diagnostics) Set(name string, value string, rate float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.metrics[name] = value
}

// Timing no-op.
func (d *Diagnostics) Timing(name string, value time.Duration, rate float64) {
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
