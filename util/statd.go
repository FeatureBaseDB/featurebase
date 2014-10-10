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
	timer chan args
	count chan string
	end   chan bool
)

func init() {
	timer = make(chan args, 100)
	count = make(chan string, 100)
	end = make(chan bool)
	stat_config := config.GetStringDefault("statsd_server", "127.0.0.1:8125")
	log.Println("New Stats", stat_config)
	stats, _ := statsd.New(stat_config, "")
	go func() {
		for {
			select {
			case ci := <-timer:
				stats.Gauge(ci.stat, ci.delta, ci.rate)
			case stat := <-count:
				stats.Inc(stat, 1, 1.0)
			case <-end:
				log.Println("DONE Stats")
				return

			}
		}
	}()
}

func SendTimer(stat string, delta int64) {
	pstat := "pilosa." + stat
	timer <- args{pstat, delta, 1.0}
}
func SendInc(stat string) {
	pstat := "pilosa." + stat
	count <- pstat
}

func ShutdownStats() {
	log.Println("Shutdown Stats")
	end <- true
}
