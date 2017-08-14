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
* Env: `PILOSA_ANTI_ENTROPY.INTERVAL="10m0s"`
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

#### Cluster Hosts

* Description: List of hosts in the cluster. Multiple hosts should be comma separated in the flag and env forms.
* Flag: `--cluster.hosts="localhost:10101"`
* Env: `PILOSA_CLUSTER.HOSTS="localhost:10101"`
* Config:

    ```toml
    [cluster]
    hosts = ["localhost:10101"]
    ```

#### Cluster Internal Hosts

* Description: List of hosts in the cluster used for internal communication. Multiple hosts should be comma separated in the flag and env forms.
* Flag: `--cluster.internal-hosts="localhost:11101"`
* Env: `PILOSA_CLUSTER.INTERNAL_HOSTS="localhost:11101"`
* Config:

    ```toml
    [cluster]
    internal-hosts = ["localhost:11101"]
    ```

#### Cluster Internal Port

* Description: Port to which Pilosa should bind for internal communication.
* Flag: `--cluster.internal-port=11101`
* Env: `PILOSA_CLUSTER.INTERNAL_PORT=11101`
* Config:

    ```toml
    [cluster]
    internal-port = 11101
    ```

#### Cluster Poll Interval

* Description: Polling interval for cluster.
* Flag: `cluster.poll-interval="1m0s"`
* Env: `PILOSA_CLUSTER.POLL_INTERVAL="1m0s"`
* Config:

    ```toml
    [cluster]
    poll-interval = "1m0s"
    ```

#### Cluster Replicas

* Description: Number of hosts each piece of data should be stored on. 
* Flag: `cluster.replicas=1`
* Env: `PILOSA_CLUSTER.REPLICAS=1`
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
* Env: `PILOSA_CLUSTER.TYPE="gossip"`
* Config:

    ```toml
    [cluster]
    type = "gossip"
    ```

#### Data Dir

* Description: Directory to store Pilosa data files.
* Flag: `--data-dir="~/.pilosa"`
* Env: `PILOSA_DATA_DIR="~/.pilosa"`
* Config:

    ```toml
    data-dir = "~/.pilosa"
    ```

#### Profile CPU

* Description: If this is set to a path, collect a cpu profile and store it there.
* Flag: `--profile.cpu="/path/to/somewhere"`
* Env: `PILOSA_PROFILE.CPU="/path/to/somewhere"`
* Config:

    ```toml
    [profile]
    cpu = "/path/to/somewhere"    
    ```

#### Profile CPU Time

* Description: Amount of time to collect cpu profiling data if `profile.cpu` is set.
* Flag: `--profile.cpu-time="30s"`
* Env: `PILOSA_PROFILE.CPU_TIME="30s"
* Config:

    ```toml
    [profile]
    cpu-time = "30s"
    ```
##### Metric Service
* Description: Which stats service to use (StatsD or ExpVar).
* Flag: `--metric.service=statsd`
* Env: `PILOSA_METRIC.SERVICE=statsd'
* Config:

    ```toml
    [metric]
    service = “statsd”
    ```

##### Metric Host
* Description: Address of the StatsD service host.
* Flag: `--metric.host=localhost:8125`
* Env: `PILOSA_METRIC.HOST=localhost:8125'
* Config:

    ```toml
    [metric]
    host = "localhost:8125"
    ```

##### Metric Poll Interval

* Description: Polling interval for runtime metrics.
* Flag: `metric.poll-interval=”0m15s”`
* Env: `PILOSA_METRIC.POLL_INTERVAL=0m15s`
* Config:

    ```toml
    [metric]
    poll-interval = "0m15s"
    ```