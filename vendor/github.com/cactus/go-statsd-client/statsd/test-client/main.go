package main

import (
	"github.com/cactus/go-statsd-client/statsd"
	flags "github.com/jessevdk/go-flags"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {

	// command line flags
	var opts struct {
		HostPort  string        `long:"host" default:"127.0.0.1:8125" description:"host:port of statsd server"`
		Prefix    string        `long:"prefix" default:"test-client" description:"Statsd prefix"`
		StatType  string        `long:"type" default:"count" description:"stat type to send. Can be timing, count, guage"`
		StatValue int64         `long:"value" default:"1" description:"Value to send"`
		Name      string        `short:"n" long:"name" default:"counter" description:"stat name"`
		Rate      float32       `short:"r" long:"rate" default:"1.0" description:"sample rate"`
		Volume    int           `short:"c" long:"count" default:"1000" description:"Number of stats to send. Volume."`
		Noop      bool          `long:"noop" default:"false" description:"Use noop client"`
		Duration  time.Duration `short:"d" long:"duration" default:"10s" description:"How long to spread the volume across. Each second of duration volume/seconds events will be sent."`
	}

	// parse said flags
	_, err := flags.Parse(&opts)
	if err != nil {
		if e, ok := err.(*flags.Error); ok {
			if e.Type == flags.ErrHelp {
				os.Exit(0)
			}
		}
		fmt.Printf("Error: %+v\n", err)
		os.Exit(1)
	}

	var client statsd.Statter
	if !opts.Noop {
		client, err = statsd.New(opts.HostPort, opts.Prefix)
		if err != nil {
			log.Fatal(err)
		}
		defer client.Close()
	} else {
		client, err = statsd.NewNoop(opts.HostPort, opts.Prefix)
	}

	var stat func(stat string, value int64, rate float32) error
	switch opts.StatType {
	case "count":
		stat = func(stat string, value int64, rate float32) error {
			return client.Inc(stat, value, rate)
		}
	case "gauge":
		stat = func(stat string, value int64, rate float32) error {
			return client.Gauge(stat, value, rate)
		}
	case "timing":
		stat = func(stat string, value int64, rate float32) error {
			return client.Timing(stat, value, rate)
		}
	default:
		log.Fatal("Unsupported state type")
	}

	pertick := opts.Volume / int(opts.Duration.Seconds()) / 10
	// add some extra tiem, because the first tick takes a while
	ender := time.After(opts.Duration + 100*time.Millisecond)
	c := time.Tick(time.Second / 10)
	count := 0
	for {
		select {
		case <-c:
			for x := 0; x < pertick; x++ {
				err := stat(opts.Name, opts.StatValue, opts.Rate)
				if err != nil {
					log.Printf("Got Error: %+v", err)
					break
				}
				count += 1
			}
		case <-ender:
			log.Printf("%d events called", count)
			os.Exit(0)
			return
		}
	}
}
