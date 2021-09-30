module github.com/molecula/featurebase/v2

replace go.etcd.io/etcd => github.com/molecula/etcd v0.0.0-20210930172242-ad94b354f72c

require (
	github.com/CAFxX/gcnotifier v0.0.0-20190112062741-224a280d589d
	github.com/DataDog/datadog-go v2.2.0+incompatible
	github.com/HdrHistogram/hdrhistogram-go v1.1.0 // indirect
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d
	github.com/beevik/ntp v0.3.0
	github.com/benbjohnson/immutable v0.3.0
	github.com/beorn7/perks v1.0.0
	github.com/buger/jsonparser v1.1.1
	github.com/cespare/xxhash v1.1.0
	github.com/coreos/go-semver v0.3.0
	github.com/coreos/go-systemd v0.0.0-20190321100706-95778dfbb74e
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f
	github.com/davecgh/go-spew v1.1.1
	github.com/desertbit/timer v0.0.0-20180107155436-c41aec40b27f
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/dustin/go-humanize v1.0.0
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/glycerine/goconvey v0.0.0-20190410193231-58a59202ab31 // indirect
	github.com/glycerine/idem v0.0.0-20190127113923-7a8083893311
	github.com/go-ole/go-ole v1.2.4
	github.com/go-test/deep v1.0.7
	github.com/gogo/protobuf v1.3.2
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/protobuf v1.3.3
	github.com/google/btree v1.0.0
	github.com/google/go-cmp v0.5.5
	github.com/google/uuid v1.1.4
	github.com/gopherjs/gopherjs v0.0.0-20200217142428-fce0ec30dd00 // indirect
	github.com/gorilla/handlers v1.3.0
	github.com/gorilla/mux v1.7.0
	github.com/gorilla/websocket v1.4.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.9.5
	github.com/improbable-eng/grpc-web v0.13.0
	github.com/jonboulle/clockwork v0.1.0
	github.com/json-iterator/go v1.1.7
	github.com/lib/pq v1.8.0
	github.com/matttproud/golang_protobuf_extensions v1.0.1
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd
	github.com/modern-go/reflect2 v1.0.1
	github.com/molecula/apophenia v0.0.0-20190827192002-68b7a14a478b
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pelletier/go-toml v1.4.0
	github.com/pkg/errors v0.9.1
	github.com/pmezard/go-difflib v1.0.0
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/client_model v0.1.0
	github.com/prometheus/common v0.7.0
	github.com/prometheus/procfs v0.0.2
	github.com/prometheus/prom2json v1.3.0
	github.com/rakyll/statik v0.1.7
	github.com/remyoudompheng/bigfft v0.0.0-20190728182440-6a916e37a237
	github.com/rs/cors v1.7.0
	github.com/satori/go.uuid v1.2.0
	github.com/shirou/gopsutil/v3 v3.20.11
	github.com/sirupsen/logrus v1.4.2
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/cobra v1.1.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.7.0
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	github.com/uber/jaeger-lib v2.4.0+incompatible
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2
	github.com/zeebo/blake3 v0.1.1
	go.etcd.io/bbolt v1.3.5
	go.etcd.io/etcd v0.0.0-20201125193152-8a03d2e9614b
	go.uber.org/atomic v1.4.0
	go.uber.org/multierr v1.1.0
	go.uber.org/zap v1.10.0
	golang.org/x/crypto v0.0.0-20200622213623-75b288015ac9
	golang.org/x/exp v0.0.0-20201008143054-e3b2a7f2fdc7
	golang.org/x/mod v0.4.2
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210510120138-977fb7262007
	golang.org/x/text v0.3.5
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
	google.golang.org/genproto v0.0.0-20191108220845-16a3f7862a1a
	google.golang.org/grpc v1.28.0
	gopkg.in/yaml.v2 v2.3.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c
	modernc.org/mathutil v1.0.0
	modernc.org/strutil v1.0.0
	sigs.k8s.io/yaml v1.2.0
	vitess.io/vitess v3.0.0-rc.3.0.20190602171040-12bfde34629c+incompatible
)

go 1.14
