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

Pilosa can be configured through command line flags, environment variables, and/or a configuration file; configured options take precedence in that order. So if an option is specified in a command line flag, it will take precedence over the same option specified in the environment, which would take precedence over that same option specified in the configuration file.

All options are available in all three configuration types with the exception of the `--config` option which specifies the location of the config file, and therefore will not be used if it is present in the config file.

The syntax for each option is slightly different between each of the configuration types, but follows a simple formula. See the following three sections for an explanation of each configuration type.

### Command line flags

Pilosa uses GNU/POSIX style flags. Most flags you specify as `--flagname=value` although some have a short form that is a single character and can be specified with a single dash like `-f value`. Running `pilosa server --help` will give an overview of the available flags as well as their short forms (if applicable).

### Environment variables

Every command line flag has a corresponding environment variable. The environment variable is the flag name in all caps, prefxed by `PILOSA_`, and with any dashes replaced by underscores. For example: `--flag-name` becomes `PILOSA_FLAG_NAME`.

### Config file

The config file is in the [toml format](https://github.com/toml-lang/toml) and has exactly the same options available as the flags and environment variables. Any flag which contains a dot (".") denotes nesting within the config file, so the two flags `--cluster.poll-interval=2m0s` and `--cluster.replicas=1` look like this in the config file:
```toml
[cluster]
  poll-interval = "2m0s"
  replicas = 1
```

Any flag that has a value that is a comma separated list on the command line becomes an array in toml. For example `--cluster.hosts=one.pilosa.com:10101,two.pilosa.com:10101` becomes:
```toml
[cluster]
  hosts = ["one.pilosa.com:10101", "two.pilosa.com:10101"]
```

### All Options

#### Anti Entropy Interval

* Description: Interval at which the cluster will run its anti-entropy routine which makes sure that all replicas of each fragment are in sync.
* Flag: `--anti-entropy.interval="10m0s"`
* Env: `PILOSA_ANTI_ENTROPY_INTERVAL="10m0s"`
* Config:

    ```toml
    [anti-entropy]
    interval = "10m0s"
    ```

#### Bind

* Description: host:port on which the Pilosa server will listen for requests. Host defaults to localhost and port to 10101.
* Flag: `--bind="localhost:10101"`
* Env: `PILOSA_BIND="localhost:10101"`
* Config:

    ```toml
    bind = localhost:10101
    ```

#### Data Dir

* Description: Directory to store Pilosa data files.
* Flag: `--data-dir="~/.pilosa"`
* Env: `PILOSA_DATA_DIR="~/.pilosa"`
* Config:

    ```toml
    data-dir = "~/.pilosa"
    ```

#### Gossip Port

* Description: Port to which Pilosa should bind for internal communication.
* Flag: `--gossip-port=11101`
* Env: `PILOSA_GOSSIP_PORT=11101`
* Config:

    ```toml
    gossip-port = 11101
    ```

#### Gossip Seed

* Description: When using the gossip [Cluster Type]({{< ref "#cluster-type" >}}), this specifies which internal host should be used to initialize membership in the cluster. Typcially this can be the address of any available host in the cluster. For example, when starting a three-node cluster made up of `node0`, `node1`, and `node2`, the `gossip-seed` for all three nodes can be configured to be the address of `node0`.
* Flag: `--gossip-seed="localhost:11101"`
* Env: `PILOSA_GOSSIP_SEED="localhost:11101"`
* Config:

    ```toml
    gossip-seed = "localhost:11101"
    ```

#### Cluster Hosts

* Description: List of hosts in the cluster. Multiple hosts should be comma separated in the flag and env forms.
* Flag: `--cluster.hosts="localhost:10101"`
* Env: `PILOSA_CLUSTER_HOSTS="localhost:10101"`
* Config:

    ```toml
    [cluster]
    hosts = ["localhost:10101"]
    ```

#### Cluster Poll Interval

* Description: Polling interval for cluster.
* Flag: `cluster.poll-interval="1m0s"`
* Env: `PILOSA_CLUSTER_POLL_INTERVAL="1m0s"`
* Config:

    ```toml
    [cluster]
    poll-interval = "1m0s"
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

* Description: Determine how the cluster handles membership and state sharing. Choose from [static, http, gossip].
  * static - Messaging between nodes is disabled. This is primarily used for testing.
  * http - Messages are transmitted over HTTP.
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

* Description: Amount of time to collect cpu profiling data if `profile.cpu` is set.
* Flag: `--profile.cpu-time="30s"`
* Env: `PILOSA_PROFILE_CPU_TIME="30s"
* Config:

    ```toml
    [profile]
    cpu-time = "30s"
    ```
##### Metric Service
* Description: Which stats service to use (StatsD or ExpVar).
* Flag: `--metric.service=statsd`
* Env: `PILOSA_METRIC_SERVICE=statsd'
* Config:

    ```toml
    [metric]
    service = “statsd”
    ```

##### Metric Host
* Description: Address of the StatsD service host.
* Flag: `--metric.host=localhost:8125`
* Env: `PILOSA_METRIC_HOST=localhost:8125'
* Config:

    ```toml
    [metric]
    host = "localhost:8125"
    ```

##### Metric Poll Interval

* Description: Polling interval for runtime metrics.
* Flag: `metric.poll-interval=”0m15s”`
* Env: `PILOSA_METRIC_POLL_INTERVAL=0m15s`
* Config:

    ```toml
    [metric]
    poll-interval = "0m15s"
    ```

##### Metric Diagnostics Interval

* Description: Diagnostic reporting interval. To disable diagnostics set to zero.
* Flag: `metric.diagnostics=”60m0s”`
* Env: `PILOSA_METRIC_DIAGNOSTICS=60m0s`
* Config:

    ```toml
    [metric]
    diagnostics = "60m0s"
    ```


##### TLS Certificate

* Description: Path to the TLS certificate to use for serving HTTPS. Usually has one of`.crt` or `.pem` extensions.
* Flag: `tls.certificate=/srv/pilosa/certs/server.crt`
* Env: `PILOSA_TLS_CERTIFICATE=/srv/pilosa/certs/server.crt`
* Config:

    ```toml
    [tls]
    certificate = "/srv/pilosa/certs/server.crt"
    ```

##### TLS Certificate Key

* Description: Path to the TLS certificate key to use for serving HTTPS. Usually has the `.key` extension.
* Flag: `tls.key=/srv/pilosa/certs/server.key`
* Env: `PILOSA_TLS_KEY=/srv/pilosa/certs/server.key`
* Config:

    ```toml
    [tls]
    key = "/srv/pilosa/certs/server.key"
    ```

##### TLS Skip Verify

* Description: Disables verification for checking TLS certificates. This configuration item is mainly useful for using self-signed certificates for a Pilosa cluster. Do not use in production since it makes man-in-the-middle attacks trivial.
* Flag: `tls.skip-verify`
* Env: `PILOSA_TLS_SKIP_VERIFY`
* Config:

    ```toml
    [tls]
    skip-verify = true
    ```

### Example Cluster Configuration

A three node cluster could be minimally configured as follows:

#### Node 0

    data-dir = "/home/pilosa/data"
    bind = "node0.pilosa.com:10101"
    gossip-port = 12000
    gossip-seed = "node0.pilosa.com:12000"

    [cluster]
      replicas = 1
      type = "gossip"
      hosts = ["node0.pilosa.com:10101","node1.pilosa.com:10101","node2.pilosa.com:10101"]

#### Node 1

    data-dir = "/home/pilosa/data"
    bind = "node1.pilosa.com:10101"
    gossip-port = 12000
    gossip-seed = "node0.pilosa.com:12000"

    [cluster]
      replicas = 1
      type = "gossip"
      hosts = ["node0.pilosa.com:10101","node1.pilosa.com:10101","node2.pilosa.com:10101"]

#### Node 2

    data-dir = "/home/pilosa/data"
    bind = "node2.pilosa.com:10101"
    gossip-port = 12000
    gossip-seed = "node0.pilosa.com:12000"

    [cluster]
      replicas = 1
      type = "gossip"
      hosts = ["node0.pilosa.com:10101","node1.pilosa.com:10101","node2.pilosa.com:10101"]


### Example Cluster Configuration (HTTPS)

The same cluster which uses HTTPS instead of HTTP can be configured as follows. Note that we explicitly specify `https` as the protocol in `bind` and `cluster.hosts` configuration: 

#### Node 0

    data-dir = "/home/pilosa/data"
    bind = "https://node0.pilosa.com:10101"
    gossip-port = 12000
    gossip-seed = "node0.pilosa.com:12000"

    [cluster]
      replicas = 1
      type = "gossip"
      hosts = ["https://node0.pilosa.com:10101","https://node1.pilosa.com:10101","https://node2.pilosa.com:10101"]

    [tls]
      certificate = "/home/pilosa/private/server.crt"
      key = "/home/pilosa/private/server.key"

#### Node 1

    data-dir = "/home/pilosa/data"
    bind = "https://node1.pilosa.com:10101"
    gossip-port = 12000
    gossip-seed = "node0.pilosa.com:12000"

    [cluster]
      replicas = 1
      type = "gossip"
      hosts = ["https://node0.pilosa.com:10101","https://node1.pilosa.com:10101","https://node2.pilosa.com:10101"]

    [tls]
      certificate = "/home/pilosa/private/server.crt"
      key = "/home/pilosa/private/server.key"
      
#### Node 2

    data-dir = "/home/pilosa/data"
    bind = "https://node2.pilosa.com:10101"
    gossip-port = 12000
    gossip-seed = "node0.pilosa.com:12000"

    [cluster]
      replicas = 1
      type = "gossip"
      hosts = ["https://node0.pilosa.com:10101","https://node1.pilosa.com:10101","https://node2.pilosa.com:10101"]

    [tls]
      certificate = "/home/pilosa/private/server.crt"
      key = "/home/pilosa/private/server.key"
