module github.com/molecula/featurebase/v2

replace go.etcd.io/etcd => github.com/molecula/etcd v0.0.0-20210621160528-2cd93f1df0e7

require (
	github.com/CAFxX/gcnotifier v0.0.0-20190112062741-224a280d589d
	github.com/DataDog/datadog-go v2.2.0+incompatible
	github.com/HdrHistogram/hdrhistogram-go v1.1.0 // indirect
	github.com/beevik/ntp v0.3.0
	github.com/benbjohnson/immutable v0.3.0
	github.com/buger/jsonparser v1.1.1
	github.com/cespare/xxhash v1.1.0
	github.com/davecgh/go-spew v1.1.1
	github.com/desertbit/timer v0.0.0-20180107155436-c41aec40b27f // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/glycerine/goconvey v0.0.0-20190410193231-58a59202ab31 // indirect
	github.com/glycerine/idem v0.0.0-20190127113923-7a8083893311
	github.com/go-test/deep v1.0.7
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.3.3
	github.com/google/go-cmp v0.5.5
	github.com/google/uuid v1.1.4 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20200217142428-fce0ec30dd00 // indirect
	github.com/gorilla/handlers v1.3.0
	github.com/gorilla/mux v1.7.0
	github.com/improbable-eng/grpc-web v0.13.0
	github.com/lib/pq v1.8.0
	github.com/molecula/apophenia v0.0.0-20190827192002-68b7a14a478b
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pelletier/go-toml v1.4.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/client_model v0.1.0
	github.com/prometheus/prom2json v1.3.0
	github.com/rakyll/statik v0.1.7
	github.com/remyoudompheng/bigfft v0.0.0-20190728182440-6a916e37a237 // indirect
	github.com/rs/cors v1.7.0 // indirect
	github.com/satori/go.uuid v1.2.0
	github.com/shirou/gopsutil/v3 v3.20.11
	github.com/spf13/cobra v1.1.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.7.0
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	github.com/uber/jaeger-lib v2.4.0+incompatible // indirect
	github.com/zeebo/blake3 v0.1.1
	go.etcd.io/bbolt v1.3.5
	go.etcd.io/etcd v0.0.0-20201125193152-8a03d2e9614b
	golang.org/x/exp v0.0.0-20201008143054-e3b2a7f2fdc7
	golang.org/x/mod v0.4.2
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210510120138-977fb7262007 // indirect
	golang.org/x/text v0.3.5 // indirect
	google.golang.org/grpc v1.28.0
	gopkg.in/yaml.v2 v2.3.0 // indirect
	modernc.org/mathutil v1.0.0
	modernc.org/strutil v1.0.0
	sigs.k8s.io/yaml v1.2.0 // indirect
	vitess.io/vitess v3.0.0-rc.3.0.20190602171040-12bfde34629c+incompatible
)

go 1.14
