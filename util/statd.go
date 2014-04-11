package util

import (
	"log"
	"pilosa/config"

	"github.com/cactus/go-statsd-client/statsd"
)

type args struct {
	stat  string
	delta int64
	rate  float32
}

var (
	count chan args
	end   chan bool
)

func init() {
	count = make(chan args, 100)
	end = make(chan bool)
	stat_config := config.GetStringDefault("statsd_server", "127.0.0.1:8125")
	log.Println("New Stats", stat_config)
	stats, _ := statsd.New(stat_config, "")
	go func() {
		for {
			select {
			case ci := <-count:
				stats.Gauge(ci.stat, ci.delta, ci.rate)

				break
			case <-end:
				log.Println("DONE Stats")
				return

			}
		}
	}()
}

func SendTimer(stat string, delta int64) {
	pstat := "pilosa." + stat
	count <- args{pstat, delta, 1.0}
}

func ShutdownStats() {
	log.Println("Shutdown Stats")
	end <- true
}
