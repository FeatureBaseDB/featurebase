module github.com/pilosa/pilosa/v2

replace github.com/hashicorp/memberlist => github.com/pilosa/memberlist v0.1.4-0.20190415211605-f6512523c021

require (
	github.com/CAFxX/gcnotifier v0.0.0-20190112062741-224a280d589d
	github.com/DataDog/datadog-go v0.0.0-20180822151419-281ae9f2d895
	github.com/benbjohnson/immutable v0.3.0
	github.com/cespare/xxhash v1.1.0
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/desertbit/timer v0.0.0-20180107155436-c41aec40b27f // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/glycerine/goconvey v0.0.0-20190410193231-58a59202ab31 // indirect
	github.com/glycerine/idem v0.0.0-20190127113923-7a8083893311
	github.com/glycerine/vprint v0.0.0-20200730000117-76cea49a68ea // indirect
	github.com/gogo/protobuf v1.2.1
	github.com/golang/protobuf v1.4.2
	github.com/google/go-cmp v0.5.2
	github.com/gopherjs/gopherjs v0.0.0-20200217142428-fce0ec30dd00 // indirect
	github.com/gorilla/handlers v1.3.0
	github.com/gorilla/mux v1.7.0
	github.com/hashicorp/memberlist v0.1.3
	github.com/improbable-eng/grpc-web v0.13.0
	github.com/kr/text v0.2.0 // indirect
	github.com/lib/pq v1.8.0
	github.com/molecula/apophenia v0.0.0-20190827192002-68b7a14a478b
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pelletier/go-toml v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/client_model v0.1.0
	github.com/prometheus/prom2json v1.3.0
	github.com/rakyll/statik v0.1.7
	github.com/remyoudompheng/bigfft v0.0.0-20190728182440-6a916e37a237 // indirect
	github.com/rs/cors v1.7.0 // indirect
	github.com/satori/go.uuid v1.2.0
	github.com/shirou/gopsutil/v3 v3.20.11
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cobra v1.1.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	github.com/zeebo/blake3 v0.0.4
	go.etcd.io/bbolt v1.3.5
	golang.org/x/exp v0.0.0-20201008143054-e3b2a7f2fdc7
	golang.org/x/mod v0.3.1-0.20200828183125-ce943fd02449
	golang.org/x/net v0.0.0-20200822124328-c89045814202 // indirect
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/sys v0.0.0-20201214095126-aec9a390925b // indirect
	golang.org/x/text v0.3.3 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/grpc v1.28.0
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/yaml.v2 v2.3.0 // indirect
	modernc.org/mathutil v1.0.0
	modernc.org/strutil v1.0.0
	vitess.io/vitess v3.0.0-rc.3.0.20190602171040-12bfde34629c+incompatible
)

go 1.14
