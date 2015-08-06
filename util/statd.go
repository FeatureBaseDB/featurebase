package util

import (
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	log "github.com/cihub/seelog"
	"github.com/umbel/pilosa/config"
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

func SetupUtil() {
	setup_storage()
	timer = make(chan args, 32768)
	count = make(chan string, 32768)
	end = make(chan bool)
	stat_config := config.GetStringDefault("statsd_server", "127.0.0.1:8125")
	log.Warn("New Stats", stat_config)
	stats, _ := statsd.New(stat_config, "")
	go func() {
		for {
			select {
			case ci := <-timer:
				stats.Gauge(ci.stat, ci.delta, ci.rate)
				stats.Timing(ci.stat, ci.delta, ci.rate)
			case stat := <-count:
				stats.Inc(stat, 1, 1.0)
			case <-end:
				log.Warn("DONE Stats")
				return

			}
		}
	}()
}

func SendTimer(stat string, delta int64) {
	pstat := "pilosa." + stat
	milli := time.Duration(delta) / time.Millisecond
	timer <- args{pstat, int64(milli), 1.0}
}
func SendInc(stat string) {
	pstat := "pilosa." + stat
	count <- pstat
}

func ShutdownStats() {
	log.Warn("Shutdown Stats")
	end <- true
}
