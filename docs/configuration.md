+++
title = "Configuration"
weight = 7
nav = [
    "Command line flags",
    "Environment variables",
    "Config file",
    "All Options",
]
+++

## Configuration

Pilosa can be configured through command line flags, environment variables, and/or a configuration file; configured options take precedence in that order. So if an option is specified in a command line flag, it will take precedence over the same option specified in the environment, which will take precedence over that same option specified in the configuration file.

All options are available in all three configuration types with the exception of the `--config` option which specifies the location of the config file, and therefore will not be used if it is present in the config file.

The syntax for each option is slightly different between each of the configuration types, but follows a simple formula. See the following three sections for an explanation of each configuration type.

### Command line flags

Pilosa uses GNU/POSIX style flags. Most flags you specify as `--flagname=value` although some have a short form that is a single character and can be specified with a single dash like `-f value`. Running `pilosa server --help` will give an overview of the available flags as well as their short forms (if applicable).

### Environment variables

Every command line flag has a corresponding environment variable. The environment variable is the flag name in all caps, prefixed by `PILOSA_`, and with dots and dashes replaced by underscores. For example: `--scope.flag-name` becomes `PILOSA_SCOPE_FLAG_NAME`.

### Config file

The config file is in the [toml format](https://github.com/toml-lang/toml) and has exactly the same options available as the flags and environment variables. Any flag which contains a dot (".") denotes nesting within the config file, so the two flags `--cluster.coordinator` and `--cluster.replicas=1` look like this in the config file:
```toml
[cluster]
  coordinator = true
  replicas = 1
```

### All Options

#### Advertise

* Description: Address advertised by the server to other nodes in the cluster and to clients via the `/status` endpoint. Host defaults to the IP address represented by `bind` and port to 10101. If `bind` is set to `0.0.0.0` and `advertise` is not specified, then Pilosa will try to determine a reasonable, external IP address to use for `advertise`.
* Flag: `--advertise="192.168.1.100:10101"`
* Env: `PILOSA_BIND="192.168.1.100:10101"`
* Config:

    ```toml
    advertise = 192.168.1.100:10101
    ```

#### Anti Entropy Interval

* Description: Interval at which the cluster will run its anti-entropy routine which ensures that all replicas of each fragment are in sync.
* Flag: `--anti-entropy.interval="10m0s"`
* Env: `PILOSA_ANTI_ENTROPY_INTERVAL="10m0s"`
* Config:

    ```toml
    [anti-entropy]
    interval = "10m0s"
    ```

#### Bind

* Description: host:port on which the Pilosa server will listen for requests. Host defaults to localhost and port to 10101. If `bind` is set to `0.0.0.0` then Pilosa will listen on all available interfaces.
* Flag: `--bind="localhost:10101"`
* Env: `PILOSA_BIND="localhost:10101"`
* Config:

    ```toml
    bind = localhost:10101
    ```

#### CORS (Cross-Origin Resource Sharing) Allowed Origins

* Description: List of allowed origin URIs for CORS
* Flag: `--handler.allowed-origins="https://myapp.com,https://myapp.org"`
* Env: `PILOSA_HANDLER_ALLOWED_ORIGINS="https://myapp.com,https://myapp.org"`
* Config:

    ```toml
    [handler]
    allowed-origins = ["https://myapp.com", "https://myapp.org"]
    ```

#### Data Dir

* Description: Directory to store Pilosa data files.
* Flag: `--data-dir="~/.pilosa"`
* Env: `PILOSA_DATA_DIR="~/.pilosa"`
* Config:

    ```toml
    data-dir = "~/.pilosa"
    ```

#### Log Path

* Description: Path of log file.
* Flag: `--log-path="/path/to/logfile"`
* Env: `PILOSA_LOG_PATH="/path/to/logfile"`
* Config:

    ```toml
    log-path = "/path/to/logfile"
    ```

#### Verbose

* Description: Enable verbose logging.
* Flag: `--verbose`
* Env: `PILOSA_VERBOSE`
* Config:

    ```toml
    verbose = true
    ```

#### Max Map Count

* Description: Maximum number of active memory maps Pilosa will use for fragment
  files (actual total usage may be slightly higher). Best practice is to set
  this ~10% lower than your system's maximum map count (obtained via `sysctl
  vm.max_map_count` on Linux). If you plan on having lots of fragments per host,
  it's a good idea to raise both the system's max map count, and Pilosa's. The
  number of fragments is a function of the number of shards, fields, and time
  quantums. Using, for example, YMDH time quantum fields with a wide range of
  timestamps will create lots of fragments. When Pilosa exhausts the
  max-map-count it falls back to reading files directly into memory. This can be
  a bit slower, and cause slower restarts, but is generally fine.
  * Flag: `--max-map-count=1000000`
  * Env: `PILOSA_MAX_MAP_COUNT=1000000`
  * Config:

      ```toml
      max-map-count = 1000000
      ```

#### Max Writes Per Request

* Description: Maximum number of mutating commands allowed per request. This includes Set, Clear, SetRowAttrs, and SetColumnAttrs.
* Flag: `--max-writes-per-request=5000`
* Env: `PILOSA_MAX_WRITES_PER_REQUEST=5000`
* Config:

    ```toml
    max-writes-per-request = 5000
    ```

#### Max File Count

* Description: A soft limit on the maximum number of files that Pilosa will keep
  open simultaneously. When past this limit, Pilosa will only keep files open
  for as long as it needs to write updates. This will negatively affect
  performance in cases where Pilosa is doing lots of small updates.
* Flag: `--max-file-count=1000000`
* Env: `PILOSA_MAX_FILE_COUNT=1000000`
* Config:

    ```toml
    max-file-count = 1000000
    ```

#### Gossip Advertise Host

* Description: Host on which memberlist should advertise. Defaults to `advertise` host.
* Flag: `--gossip.advertise-host=192.168.1.100`
* Env: `PILOSA_GOSSIP_ADVERTISE_HOST=192.168.1.100
* Config:

    ```toml
    [gossip]
      advertise-host = 192.168.1.100
    ```

#### Gossip Advertise Port

* Description: Port on which memberlist should advertise. Defaults to `advertise` port.
* Flag: `--gossip.advertise-port=15001`
* Env: `PILOSA_GOSSIP_ADVERTISE_PORT=15001`
* Config:

    ```toml
    [gossip]
      advertise-port = 15001
    ```

#### Gossip Port

* Description: Port to which Pilosa should bind for internal communication. If more than one Pilosa server is running on the same host, the gossip port for each server must be unique.
* Flag: `--gossip.port=11101`
* Env: `PILOSA_GOSSIP_PORT=11101`
* Config:

    ```toml
    [gossip]
      port = 11101
    ```

#### Gossip Seeds

* Description: This specifies which internal host(s) should be used to initialize membership in the cluster. Typically this can be the address of any available host in the cluster. For example, when starting a three-node cluster made up of `node0`, `node1`, and `node2`, the `gossip.seeds` for all three nodes can be configured to be the address of `node0`. Multiple seeds should be comma-separated in the flag and env forms.
* Flag: `--gossip.seeds="localhost:11101,localhost:11110"`
* Env: `PILOSA_GOSSIP_SEEDS="localhost:11101,localhost:11110"`
* Config:

    ```toml
    [gossip]
      seeds = ["localhost:11101", "localhost:11110"]
    ```

#### Gossip Key

* Description: Path to the file which contains the key to encrypt gossip communication. The contents of the file should be either 16, 24, or 32 bytes to select AES-128, AES-192, or AES-256 encryption. You can read from `/dev/random` device on UNIX-like systems to create the key file; e.g., `head -c 32 /dev/random > gossip.key32` creates a key file to use AES-256.
* Flag: `--gossip.key="/var/secret/gossip.key32"`
* Env: `PILOSA_GOSSIP_KEY="/var/secret/gossip.key32"`
* Config:

    ```toml
    [gossip]
      key = "/var/secret/gossip.key32"
    ```

#### Cluster Coordinator

* Description: Indicates whether the node should act as the coordinator for the cluster. Only one node per cluster should be the coordinator.
* Flag: `cluster.coordinator`
* Env: `PILOSA_CLUSTER_COORDINATOR`
* Config:

    ```toml
    [cluster]
    coordinator = true
    ```

#### Cluster Long Query Time

* Description: Duration that will trigger log and stat messages for slow queries.
* Flag: `cluster.long-query-time="1m0s"`
* Env: `PILOSA_CLUSTER_LONG_QUERY_TIME="1m0s"`
* Config:

    ```toml
    [cluster]
    long-query-time = "1m0s"
    ```

#### Cluster Replicas

* Description: Number of hosts each piece of data should be stored on. 
* Flag: `cluster.replicas=1`
* Env: `PILOSA_CLUSTER_REPLICAS=1`
* Config:

    ```toml
    [cluster]
    replicas = 1
    ```

#### Cluster Type

* Description: Determine how the cluster handles membership and state sharing. Choose from [static, gossip].
  * static - Messaging between nodes is disabled. This is primarily used for testing.
  * gossip - Messages are transmitted over TCP. Cluster status and node state are kept in sync via internode gossip.
* Flag: `cluster.type="gossip"`
* Env: `PILOSA_CLUSTER_TYPE="gossip"`
* Config:

    ```toml
    [cluster]
    type = "gossip"
    ```

#### Profile CPU

* Description: If this is set to a path, collect a cpu profile and store it there.
* Flag: `--profile.cpu="/path/to/somewhere"`
* Env: `PILOSA_PROFILE_CPU="/path/to/somewhere"`
* Config:

    ```toml
    [profile]
    cpu = "/path/to/somewhere"
    ```

#### Profile CPU Time

* Description: Amount of time to collect cpu profiling data at startup if `profile.cpu` is set.
* Flag: `--profile.cpu-time="30s"`
* Env: `PILOSA_PROFILE_CPU_TIME="30s"`
* Config:

    ```toml
    [profile]
    cpu-time = "30s"
    ```

#### Metric Service
* Description: Which stats service to use for collecting [metrics](../administration/#metrics). Choose from [statsd, expvar, prometheus, none].
* Flag: `--metric.service=statsd`
* Env: `PILOSA_METRIC_SERVICE=statsd`
* Config:

    ```toml
    [metric]
    service = “statsd”
    ```

#### Metric Host
* Description: Address of the StatsD service host.
* Flag: `--metric.host=localhost:8125`
* Env: `PILOSA_METRIC_HOST=localhost:8125`
* Config:

    ```toml
    [metric]
    host = "localhost:8125"
    ```

#### Metric Poll Interval

* Description: Rate at which runtime metrics (such as open file handles and memory usage) are collected.
* Flag: `metric.poll-interval=”0m15s”`
* Env: `PILOSA_METRIC_POLL_INTERVAL=0m15s`
* Config:

    ```toml
    [metric]
    poll-interval = "0m15s"
    ```

#### Metric Diagnostics

* Description: Enable [reporting](../administration/#diagnostics) of limited usage statistics to Pilosa developers. To disable, set to false.
* Flag: `metric.diagnostics`
* Env: `PILOSA_METRIC_DIAGNOSTICS`
* Config:

    ```toml
    [metric]
    diagnostics = true
    ```


#### TLS Certificate

* Description: Path to the TLS certificate to use for serving HTTPS. Usually has one of `.crt` or `.pem` extensions.
* Flag: `tls.certificate=/srv/pilosa/certs/server.crt`
* Env: `PILOSA_TLS_CERTIFICATE=/srv/pilosa/certs/server.crt`
* Config:

    ```toml
    [tls]
    certificate = "/srv/pilosa/certs/server.crt"
    ```

#### TLS Certificate Key

* Description: Path to the TLS certificate key to use for serving HTTPS. Usually has the `.key` extension.
* Flag: `tls.key=/srv/pilosa/certs/server.key`
* Env: `PILOSA_TLS_KEY=/srv/pilosa/certs/server.key`
* Config:

    ```toml
    [tls]
    key = "/srv/pilosa/certs/server.key"
    ```

#### TLS CA Certificate

* Description: Path to the TLS certificate key to use for serving HTTPS. Usually has one of `.crt` or `.pem` extensions.
* Flag: `tls.ca-certificate=/srv/pilosa/certs/ca-chain.pem`
* Env: `PILOSA_TLS_CA_CERTIFICATE=/srv/pilosa/certs/ca-chain.pem`
* Config:

    ```toml
    [tls]
    ca-certificate = "/srv/pilosa/certs/ca-chain.pem"
    ```

#### TLS Skip Verify

* Description: Disables verification for checking TLS certificates. This configuration item is mainly useful for using self-signed certificates for a Pilosa cluster. Do not use in production since it makes man-in-the-middle attacks trivial.
* Flag: `tls.skip-verify`
* Env: `PILOSA_TLS_SKIP_VERIFY`
* Config:

    ```toml
    [tls]
    skip-verify = true
    ```

#### TLS Enable Client Certificate Verification

* Description: Enables verification of client certificates on incoming HTTPS requests for mutual TLS authentication.
* Flag: `tls.enable-client-verification`
* Env: `PILOSA_TLS_ENABLE_CLIENT_VERIFICATION`
* Config:

    ```toml
    [tls]
    enable-client-verification = true
    ```

#### Tracing Sampler Type

* Description: Jaeger sampler type (const, probabilistic, ratelimiting, or remote). Set to 'off' to disable tracing completely.
* Flag: `tracing.sampler-type`
* Env: `PILOSA_TRACING_SAMPLER_TYPE`
* Config:

    ```toml
    [tracing]
    sampler-type = "remote"
    ```

#### Tracing Sampler Parameter

* Description: Jaeger sampler parameter (number)
* Flag: `tracing.sampler-param`
* Env: `PILOSA_TRACING_SAMPLER_PARAM`
* Config:

    ```toml
    [tracing]
    sampler-param = 0.001
    ```

#### Tracing Agent Host/Port

* Description: Jaeger agent host:port
* Flag: `tracing.agent-host-port`
* Env: `PILOSA_TRACING_AGENT_HOST_PORT`
* Config:

    ```toml
    [tracing]
    agent-host-port = "localhost:6831"
    ```

#### Profile Block Rate

* Description: Block Rate is passed directly to Go's
  [runtime.SetBlockProfileRate](https://golang.org/pkg/runtime/#SetBlockProfileRate). Goroutine blocking events will be sampled at 1
  per `rate` nanoseconds. A value of "1" samples every event, and 0 disables
  profiling.
* Flag: `--profile.block-rate=10000000`
* Env: `PILOSA_PROFILE_BLOCK_RATE=10000000`
* Config:

   ```toml
   [profile]
   block-rate = 10000000
   ```

#### Profile Mutex Fraction

* Description: Mutex Fraction is passed directly to Go's
  [runtime.SetMutexProfileFraction](https://golang.org/pkg/runtime/#SetMutexProfileFraction). 1/`fraction` of events will be sampled. 
* Flag: `--profile.mutex-fraction=100`
* Env: `PILOSA_PROFILE_MUTEX_FRACTION=100`
* Config:

   ```toml
   [profile]
   mutex-fraction = 100
   ```

#### Translation Map Size

* Description: Size in bytes of mmap to allocate for key translation
* Flag: `translation.map-size`
* Env: `PILOSA_TRANSLATION_MAP_SIZE`
* Config:

    ```toml
    [translation]
    map-size = 10737418240
    ```

### Example Cluster Configuration

A three node cluster running on different hosts could be minimally configured as follows:

#### Node 0

    data-dir = "/home/pilosa/data"
    bind = "node0.pilosa.com:10101"
    
    [gossip]
      port = 12000
      seeds = ["node0.pilosa.com:12000"]

    [cluster]
      replicas = 1
      coordinator = true

#### Node 1

    data-dir = "/home/pilosa/data"
    bind = "node1.pilosa.com:10101"
    
    [gossip]
      port = 12000
      seeds = ["node0.pilosa.com:12000"]

    [cluster]
      replicas = 1
      coordinator = false

#### Node 2

    data-dir = "/home/pilosa/data"
    bind = "node2.pilosa.com:10101"
    
    [gossip]
      port = 12000
      seeds = ["node0.pilosa.com:12000"]

    [cluster]
      replicas = 1
      coordinator = false


### Example Cluster Configuration (HTTPS)

The same cluster which uses HTTPS instead of HTTP can be configured as follows. Note that we explicitly specify `https` as the protocol in `bind` and `cluster.hosts` configuration. It is not required to use a gossip key but it is highly recommended: 

#### Node 0

    data-dir = "/home/pilosa/data"
    bind = "https://node0.pilosa.com:10101"

    [gossip]
      port = 12000
      seeds = ["node0.pilosa.com:12000"]
      key = "/home/pilosa/private/gossip.key32"

    [cluster]
      replicas = 1
      coordinator = true

    [tls]
      certificate = "/home/pilosa/private/server.crt"
      key = "/home/pilosa/private/server.key"

#### Node 1

    data-dir = "/home/pilosa/data"
    bind = "https://node1.pilosa.com:10101"

    [gossip]
      port = 12000
      seeds = ["node0.pilosa.com:12000"]
      key = "/home/pilosa/private/gossip.key32"

    [cluster]
      replicas = 1
      coordinator = false

    [tls]
      certificate = "/home/pilosa/private/server.crt"
      key = "/home/pilosa/private/server.key"
      
#### Node 2

    data-dir = "/home/pilosa/data"
    bind = "https://node2.pilosa.com:10101"

    [gossip]
      port = 12000
      seeds = ["node0.pilosa.com:12000"]
      key = "/home/pilosa/private/gossip.key32"

    [cluster]
      replicas = 1
      coordinator = false

    [tls]
      certificate = "/home/pilosa/private/server.crt"
      key = "/home/pilosa/private/server.key"

### Example Cluster Configuration (HTTPS, same host)

You can run a cluster on the same host using the configuration above with a few changes. Gossip port and bind address should be different for each node and a data directory should be accessed only by a single node.

#### Node 0

    data-dir = "/home/pilosa/data0"
    bind = "https://localhost:10100"

    [gossip]
      port = 12000
      seeds = ["localhost:12000"]
      key = "/home/pilosa/private/gossip.key32"

    [cluster]
      replicas = 1
      coordinator = true

    [tls]
      certificate = "/home/pilosa/private/server.crt"
      key = "/home/pilosa/private/server.key"

#### Node 1

    data-dir = "/home/pilosa/data1"
    bind = "https://localhost:10101"

    [gossip]
      port = 12001
      seeds = ["localhost:12000"]
      key = "/home/pilosa/private/gossip.key32"

    [cluster]
      replicas = 1
      coordinator = false

    [tls]
      certificate = "/home/pilosa/private/server.crt"
      key = "/home/pilosa/private/server.key"
      
#### Node 2

    data-dir = "/home/pilosa/data2"
    bind = "https://localhost:10102"

    [gossip]
      port = 12002
      seeds = ["localhost:12000"]
      key = "/home/pilosa/private/gossip.key32"

    [cluster]
      replicas = 1
      coordinator = false

    [tls]
      certificate = "/home/pilosa/private/server.crt"
      key = "/home/pilosa/private/server.key"
