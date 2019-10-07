module github.com/pilosa/pilosa/v2

replace github.com/hashicorp/memberlist => github.com/pilosa/memberlist v0.1.4-0.20190415211605-f6512523c021

require (
	github.com/CAFxX/gcnotifier v0.0.0-20190112062741-224a280d589d
	github.com/DataDog/datadog-go v0.0.0-20180822151419-281ae9f2d895
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/boltdb/bolt v1.3.1
	github.com/cespare/xxhash v1.1.0
	github.com/davecgh/go-spew v1.1.1
	github.com/gogo/protobuf v1.2.0
	github.com/golang/protobuf v1.3.2
	github.com/google/go-cmp v0.2.0
	github.com/gorilla/handlers v1.3.0
	github.com/gorilla/mux v1.7.0
	github.com/hashicorp/memberlist v0.1.3
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pelletier/go-toml v1.2.0
	github.com/pilosa/pilosa v1.4.0
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v0.9.3
	github.com/prometheus/client_model v0.0.0-20190129233127-fd36f4220a90
	github.com/remyoudompheng/bigfft v0.0.0-20190728182440-6a916e37a237 // indirect
	github.com/satori/go.uuid v1.2.0
	github.com/shirou/gopsutil v2.18.12+incompatible
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.3
	github.com/spf13/viper v1.3.1
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	golang.org/x/net v0.0.0-20190424112056-4829fb13d2c6
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	google.golang.org/grpc v1.24.0
	modernc.org/mathutil v1.0.0
	modernc.org/strutil v1.0.0
)

go 1.11
