module github.com/pilosa/pilosa/v2

replace github.com/hashicorp/memberlist => github.com/pilosa/memberlist v0.1.4-0.20190415211605-f6512523c021

require (
	github.com/CAFxX/gcnotifier v0.0.0-20190112062741-224a280d589d
	github.com/DataDog/datadog-go v0.0.0-20180822151419-281ae9f2d895
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/benbjohnson/immutable v0.2.0
	github.com/boltdb/bolt v1.3.1
	github.com/cespare/xxhash v1.1.0
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/dgraph-io/badger/v2 v2.0.1-rc1.0.20200709123515-8e896a7af361
	github.com/glycerine/lmdb-go v1.9.32
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/gogo/protobuf v1.2.1
	github.com/golang/protobuf v1.3.3
	github.com/google/go-cmp v0.4.0
	github.com/gorilla/handlers v1.3.0
	github.com/gorilla/mux v1.7.0
	github.com/hashicorp/memberlist v0.1.3
	github.com/lib/pq v1.8.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pelletier/go-toml v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/client_model v0.1.0
	github.com/prometheus/prom2json v1.3.0
	github.com/rakyll/statik v0.1.7
	github.com/remyoudompheng/bigfft v0.0.0-20190728182440-6a916e37a237 // indirect
	github.com/satori/go.uuid v1.2.0
	github.com/shirou/gopsutil v2.18.12+incompatible
	github.com/shirou/w32 v0.0.0-20160930032740-bb4de0191aa4 // indirect
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.4.0
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	github.com/zeebo/blake3 v0.0.4
	golang.org/x/mod v0.3.0
	golang.org/x/net v0.0.0-20200822124328-c89045814202 // indirect
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/text v0.3.3 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/grpc v1.28.0
	modernc.org/mathutil v1.0.0
	modernc.org/strutil v1.0.0
	vitess.io/vitess v3.0.0-rc.3.0.20190602171040-12bfde34629c+incompatible
)

go 1.13
