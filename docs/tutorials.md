+++
title = "Tutorials"
weight = 4
nav = [
    "How To Setup a Secure Cluster",
]
+++

## Tutorials

### How To Setup a Secure Cluster

#### Introduction

Pilosa supports encrypting the communication between and to nodes in a cluster using TLS. In this tutorial, we will be setting up a three node Pilosa cluster running on the same computer. The same steps can be used for a multi-computer cluster but that requires setting up firewalls and other platform-specific configuration which is out of the scope of this tutorial.

This tutorial assumes that you are using a UNIX-like system, such as Linux or MacOS. [Windows Subsystem for Linux (WSL)](https://msdn.microsoft.com/en-us/commandline/wsl/about) works equally well on Windows 10 systems.

#### Installing Pilosa and Creating the Directory Structure

If you haven't already done so, install Pilosa server on your computer. For Linux and WSL (Windows Subsystem for Linux) use the [Installing on Linux](https://www.pilosa.com/docs/latest/installation/#installing-on-linux) instructions. For MacOS use the [Installing on MacOS](https://www.pilosa.com/docs/latest/installation/#installing-on-macos). We do not support precompiled releases for other platforms, but you can always compile it yourself from source. See [Build from Source](https://www.pilosa.com/docs/latest/installation/#build-from-source).

After installing Pilosa, you may have to add it to your `$PATH`. Check that you can run Pilosa from the command line:
```
pilosa --help
```

Let's create a directory for the tutorial to put all of our files and switch to that directory:
```
mkdir $HOME/pilosa-tls-tutorial && cd $_
```

#### Creating the TLS Certificate and Gossip Key

Securing a Pilosa cluster consists of securing the communication between nodes using TLS and Gossip encryption. [Pilosa Enterprise](https://www.pilosa.com/enterprise/) additionally supports authentication and other security features, but those are not covered in this tutorial.

The first step is acquiring an SSL certificate. You can buy a commercial certificate or retrieve a Let's Encrypt certificiate but we will be using a self signed certificate for practical reasons. Using self-signed certificates is not recommended in production, since it makes man in the middle attacks easy.

The following command creates a 2048bit self-signed wildcard certificate for `*.pilosa.local` which expires 10 years later.

```
openssl req -x509 -newkey rsa:2048 -keyout pilosa.local.key -out pilosa.local.crt -days 3650 -nodes -subj "/C=US/ST=Texas/L=Austin/O=Pilosa/OU=Com/CN=*.pilosa.local"
```

The command above creates two files in the current directory:
* `pilosa.local.crt` is the SSL certificate.
* `pilosa.local.key` is the private key file which must be kept as secret.

Having created the SSL certificate, we can now create the gossip encryption key. Gossip encryption key file must be exactly 16, 24, or 32 bytes to select one of AES-128, AES-192, or AES-256 encryption. Reading random bytes from cryptographically secure `/dev/random` serves our purpose very well:
```
head -c 32 /dev/random > pilosa.local.gossip32
```

We now should have `pilosa.local.gossip32` in the current directory with 32 random bytes.

#### Creating the Configuration Files

Pilosa supports passing configuration items using the command line, environment variables or a configuration file. We will use the last option in this tutorial and create three configuration files for our three nodes.

Create `node1.config.toml` in the project directory and paste the following in it:

```toml
# node1.config.toml

data-dir = "node1_data"
bind = "https://01.pilosa.local:10501"

[cluster]
hosts = ["https://01.pilosa.local:10501", "https://02.pilosa.local:10502", "https://03.pilosa.local:10503"]

[tls]
certificate = "pilosa.local.crt"
key = "pilosa.local.key"
skip-verify = true

[gossip]
seed = "01.pilosa.local:15000"
port = 15000
key = "pilosa.local.gossip32"
```

Create `node2.config.toml` in the project directory and paste the following in it:

```toml
# node2.config.toml

data-dir = "node2_data"
bind = "https://02.pilosa.local:10502"

[cluster]
hosts = ["https://01.pilosa.local:10501", "https://02.pilosa.local:10502", "https://03.pilosa.local:10503"]

[tls]
certificate = "pilosa.local.crt"
key = "pilosa.local.key"
skip-verify = true

[gossip]
seed = "01.pilosa.local:15000"
port = 16000
key = "pilosa.local.gossip32"
```

Create `node3.config.toml` in the project directory and paste the following in it:

```toml
# node3.config.toml

data-dir = "node3_data"
bind = "https://03.pilosa.local:10503"

[cluster]
hosts = ["https://01.pilosa.local:10501", "https://02.pilosa.local:10502", "https://03.pilosa.local:10503"]

[tls]
certificate = "pilosa.local.crt"
key = "pilosa.local.key"
skip-verify = true

[gossip]
seed = "01.pilosa.local:15000"
port = 17000
key = "pilosa.local.gossip32"
```

Here is some explanation of the configuration items:
* `data-dir` points to the directory where the Pilosa server writes its data. If it doesn't exist, the server will create it.
* `bind` is the address to which the server listens for incoming requests. The address is composed of three parts: scheme, host, and port. The default scheme is `http` so we explicitly specify `https` to use the HTTPS protocol for communication between nodes.
* `[cluster]` section contains the settings for a cluster. `hosts` field is the most important, which contains the list of addresses of other nodes. See [Cluster Configuration](https://www.pilosa.com/docs/latest/configuration/#cluster-hosts) for other settings.
* `[tls]` section contains the TLS settings, including the path to the SSL certificate and the corresponding key. Set `skip-verify` to `true` in order to disable host name verification and other security measures. Do not set `skip-verify` to `true` on production servers.
* `[gossip]` section contains settings for the Gossip protocol. `seed` is the host and port for the main gossip node which coordinates other nodes. The `port` setting is the gossip listen address for the node. It should be different for each node, if the cluster is running on the same computer, otherwise you can set it to the same value. Finally, the `key` points to the gossip encryption key we created before.

#### Final Touches Before Running the Cluster

Before running the cluster, let's make sure that `01.pilosa.local`, `02.pilosa.local` and `03.pilosa.local` resolve to an IP address. If you are running the cluster on your computer, it is adequate to add them to your `/etc/hosts`. Below is one of the many ways of doing that (mind the `>>`):
```
sudo sh -c 'printf "\n127.0.0.1 01.pilosa.local 02.pilosa.local 03.pilosa.local\n" >> /etc/hosts'
```

Ensure we can access the hosts in the cluster:
```
ping -c 1 01.pilosa.local
ping -c 1 02.pilosa.local
ping -c 1 03.pilosa.local
```

If any of the commands above return `ping: unknown host`, make sure your `/etc/hosts` contains the failed hostname.

#### Running the Cluster

Let's open three terminal windows and run each node in its window. This will enable us to better observe what's happening on which node.

Switch to the first terminal window, change to the project directory and start the first node:
```
cd $HOME/pilosa-tls-tutorial
pilosa server -c node1.config.toml
```

Switch to the second terminal window, change to the project directory and start the second node:
```
cd $HOME/pilosa-tls-tutorial
pilosa server -c node2.config.toml
```

Switch to the third terminal window, change to the project directory and start the third node:
```
cd $HOME/pilosa-tls-tutorial
pilosa server -c node3.config.toml
```

Let's ensure that all three Pilosa servers are runnning and they are connected:
```
curl -k --ipv4 https://01.pilosa.local:10501/status
```

The `-k` flag is used to tell curl that it shouldn't bother with checking the certificate the server provides and `--ipv4` workarounds an issue on MacOS where the curl requests take a long time if the address resolves to `127.0.0.1`. You can leave it out on Linux and WSL.

All nodes should be in the `UP` state:
```
{"status":{"Nodes":[{"Host":"01.pilosa.local:10501","State":"UP"},{"Host":"02.pilosa.local:10502","State":"UP"},{"Host":"03.pilosa.local:10503","State":"UP"}]}}
```

#### Running Queries

Having confirmed that our cluster is running OK, let's run a few queries. But before that, we need to create an index and a frame:
```
curl -k --ipv4 https://01.pilosa.local:10501/index/sample-index -d ''
```

This will create index `sample-index` with default options. Let's create the frame now:
```
curl -k --ipv4 https://01.pilosa.local:10501/index/sample-index/frame/sample-frame -d ''
```

We just created frame `sample-frame` with default options.

Let's run a `SetBit` query:
```
curl -k --ipv4 https://01.pilosa.local:10501/index/sample-index/query -d 'SetBit(frame="sample-frame", rowID=1, columnID=100)'
```

Confirm that the bit was indeed set:
```
curl -k --ipv4 https://01.pilosa.local:10501/index/sample-index/query -d 'Bitmap(frame="sample-frame", rowID=1)'
```

The same response should be returned when querying other nodes in the cluster:
```
curl -k --ipv4 https://02.pilosa.local:10502/index/sample-index/query -d 'Bitmap(frame="sample-frame", rowID=1)'
```

#### What's Next?

Check out our [Administration Guide](https://www.pilosa.com/docs/latest/administration/) to learn more about making the most of your Pilosa cluster and [Configuration Documentation](https://www.pilosa.com/docs/latest/configuration/) to see the available options to configure Pilosa.
