module github.com/pilosa/pilosa/v2

replace github.com/hashicorp/memberlist => github.com/pilosa/memberlist v0.1.4-0.20190415211605-f6512523c021

require (
	github.com/CAFxX/gcnotifier v0.0.0-20190112062741-224a280d589d
	github.com/DataDog/datadog-go v0.0.0-20180822151419-281ae9f2d895
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/benbjohnson/immutable v0.2.0
	github.com/boltdb/bolt v1.3.1
	github.com/cespare/xxhash v1.1.0
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e // indirect
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/dchest/blake2b v1.0.0 // indirect
	github.com/dgraph-io/badger v1.6.1-0.20191025180844-32a2548a9d85 // indirect
	github.com/dgraph-io/badger/v2 v2.0.1-rc1.0.20200709123515-8e896a7af361
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/gogo/protobuf v1.2.0
	github.com/golang/protobuf v1.3.3
	github.com/google/go-cmp v0.2.0
	github.com/gorilla/handlers v1.3.0
	github.com/gorilla/mux v1.7.0
	github.com/hashicorp/memberlist v0.1.3
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/molecula/ext v0.0.0-20200103203257-8a458a73e8c2
	github.com/molecula/extensions v0.0.0-20191218165536-562244600fd4
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pelletier/go-toml v1.2.0
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v0.9.3
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4
	github.com/remyoudompheng/bigfft v0.0.0-20190728182440-6a916e37a237 // indirect
	github.com/satori/go.uuid v1.2.0
	github.com/shirou/gopsutil v2.18.12+incompatible
	github.com/shirou/w32 v0.0.0-20160930032740-bb4de0191aa4 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.3
	github.com/spf13/viper v1.3.2
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	github.com/willoch/tago v0.0.0-20180311150625-8f2f8e8900dc // indirect
	github.com/zeebo/blake3 v0.0.4
	go.uber.org/atomic v1.4.0 // indirect
	golang.org/x/crypto v0.0.0-20190426145343-a29dc8fdc734 // indirect
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	golang.org/x/text v0.3.2 // indirect
	google.golang.org/grpc v1.28.0
	modernc.org/mathutil v1.0.0
	modernc.org/strutil v1.0.0
)

go 1.13
