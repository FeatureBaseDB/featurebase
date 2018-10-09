+++
title = "Tutorials"
weight = 4
nav = [
    "Setting Up a Secure Cluster",
    "Setting Up a Docker Cluster",
    "Using Integer Field Values",
    "Storing Row and Column Attributes",
]
+++

## Tutorials

<div class="note">
<!-- this is html because there is a problem putting a list inside a shortcode -->
Some of our tutorials work better as standalone repos, since you can <code>git clone</code> the instructions, code, and data all at once. Officially supported tutorials are listed here.<br />
<br />
<ul>
<li><a href="https://github.com/pilosa/cosmosa">Run Pilosa with Microsoft's Azure Cosmos DB</a></li>
</ul>

</div>

### Setting Up a Secure Cluster

#### Introduction

Pilosa supports encrypting all communication with nodes in a cluster using TLS. In this tutorial, we will be setting up a three node Pilosa cluster running on the same computer. The same steps can be used for a multi-computer cluster but that requires setting up firewalls and other platform-specific configuration which is beyond the scope of this tutorial.

This tutorial assumes that you are using a UNIX-like system, such as Linux or MacOS. [Windows Subsystem for Linux (WSL)](https://msdn.microsoft.com/en-us/commandline/wsl/about) works equally well on Windows 10 systems.

#### Installing Pilosa and Creating the Directory Structure

If you haven't already done so, install Pilosa server on your computer. For Linux and WSL (Windows Subsystem for Linux) use the [Installing on Linux](../installation/#installing-on-linux) instructions. For MacOS use the [Installing on MacOS](../installation/#installing-on-macos). We do not support precompiled releases for other platforms, but you can always compile it yourself from source. See [Build from Source](../installation/#build-from-source).

After installing Pilosa, you may have to add it to your `$PATH`. Check that you can run Pilosa from the command line:
``` request
pilosa --help
```
``` response
Pilosa is a fast index to turbocharge your database.

This binary contains Pilosa itself, as well as common
tools for administering pilosa, importing/exporting data,
backing up, and more. Complete documentation is available
at https://www.pilosa.com/docs/.

Version: v1.0.0
Build Time: 2018-05-14T22:14:01+0000

Usage:
  pilosa [command]

Available Commands:
  check           Do a consistency check on a pilosa data file.
  config          Print the current configuration.
  export          Export data from pilosa.
  generate-config Print the default configuration.
  help            Help about any command
  import          Bulk load data into pilosa.
  inspect         Get stats on a pilosa data file.
  server          Run Pilosa.

Flags:
  -c, --config string   Configuration file to read from.
  -h, --help            help for pilosa

Use "pilosa [command] --help" for more information about a command.
```

First, create a directory in which to put all of the files for this tutorial. Then switch to that directory:
```
mkdir $HOME/pilosa-tls-tutorial && cd $_
```

#### Creating the TLS Certificate and Gossip Key

Securing a Pilosa cluster consists of securing the communication between nodes using TLS and Gossip encryption. [Pilosa Enterprise](https://www.pilosa.com/enterprise/) additionally supports authentication and other security features, but those are not covered in this tutorial.

The first step is acquiring an SSL certificate. You can buy a commercial certificate or retrieve a [Let's Encrypt](https://letsencrypt.org/) certificate, but we will be using a self signed certificate for practical reasons. Using self-signed certificates is not recommended in production since it makes man-in-the-middle attacks easy.

The following command creates a 2048-bit, self-signed wildcard certificate for `*.pilosa.local` which expires 10 years later.

```
openssl req -x509 -newkey rsa:2048 -keyout pilosa.local.key -out pilosa.local.crt -days 3650 -nodes -subj "/C=US/ST=Texas/L=Austin/O=Pilosa/OU=Com/CN=*.pilosa.local"
```

The command above creates two files in the current directory:

* `pilosa.local.crt` is the SSL certificate.
* `pilosa.local.key` is the private key file which must be kept as secret.

Having created the SSL certificate, we can now create the gossip encryption key. The gossip encryption key file must be exactly 16, 24, or 32 bytes to select one of AES-128, AES-192, or AES-256 encryption. Reading random bytes from cryptographically secure `/dev/random` serves our purpose very well:
```
head -c 32 /dev/random > pilosa.local.gossip32
```

We now have a file called `pilosa.local.gossip32` in the current directory which contains 32 random bytes.

#### Creating the Configuration Files

Pilosa supports passing configuration items using command line options, environment variables, or a configuration file. For this tutorial, we will use three configuration files; one configuration file for each of our three nodes.

One of the nodes in the cluster must be chosen as the *coordinator*. We choose the first node as the coordinator in this tutorial. The coordinator is only important during cluster resizing operations, and otherwise acts like any other node in the cluster. In the future, the coordinator will be chosen transparently by distributed consensus, and this option will be deprecated.

Create `node1.config.toml` in the project directory and paste the following in it:

```toml
# node1.config.toml

data-dir = "node1_data"
bind = "https://01.pilosa.local:10501"

[cluster]
coordinator = true

[tls]
certificate = "pilosa.local.crt"
key = "pilosa.local.key"
skip-verify = true

[gossip]
seeds = ["01.pilosa.local:15000"]
port = 15000
key = "pilosa.local.gossip32"
```

Create `node2.config.toml` in the project directory and paste the following in it:

```toml
# node2.config.toml

data-dir = "node2_data"
bind = "https://02.pilosa.local:10502"

[tls]
certificate = "pilosa.local.crt"
key = "pilosa.local.key"
skip-verify = true

[gossip]
seeds = ["01.pilosa.local:15000"]
port = 16000
key = "pilosa.local.gossip32"
```

Create `node3.config.toml` in the project directory and paste the following in it:

```toml
# node3.config.toml

data-dir = "node3_data"
bind = "https://03.pilosa.local:10503"

[tls]
certificate = "pilosa.local.crt"
key = "pilosa.local.key"
skip-verify = true

[gossip]
seeds = ["01.pilosa.local:15000"]
port = 17000
key = "pilosa.local.gossip32"
```

Here is some explanation of the configuration items:

* `data-dir` points to the directory where the Pilosa server writes its data. If it doesn't exist, the server will create it.
* `bind` is the address to which the server listens for incoming requests. The address is composed of three parts: scheme, host, and port. The default scheme is `http` so we explicitly specify `https` to use the HTTPS protocol for communication between nodes.
* `[cluster]` section contains the settings for a cluster. We set `coordinator = true` for only the first node to choose that as the coordinator node. See [Cluster Configuration](../configuration/#cluster-coordinator) for other settings.
* `[tls]` section contains the TLS settings, including the path to the SSL certificate and the corresponding key. Set `skip-verify` to `true` in order to disable host name verification and other security measures. Do not set `skip-verify` to `true` on production servers.
* `[gossip]` section contains settings for the gossip protocol. `seeds` contains the list of nodes from which to seed cluster membership. There must be at least one gossip seed. The `port` setting is the gossip listen address for the node. If all nodes of the cluster are running on the same computer, the gossip listen address should be different for each node. Otherwise, it can be set to the same value. Finally, the `key` points to the gossip encryption key we created earlier.

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

Let's open three terminal windows and run each node in its own window. This will enable us to better observe what's happening on each node.

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

Let's ensure that all three Pilosa servers are running and they are connected:
``` request
curl -k --ipv4 https://01.pilosa.local:10501/status
```
``` response
{"state":"NORMAL","nodes":[{"id":"98ebd177-c082-4c54-8d48-7e7c75857b52","uri":{"scheme":"https","host":"02.pilosa.local","port":10502},"isCoordinator":false},{"id":"a33dc0d6-c35f-4559-984a-e582bf032a21","uri":{"scheme":"https","host":"03.pilosa.local","port":10503},"isCoordinator":false},{"id":"e24ac014-ee2f-4cb0-b565-74df6c551f0a","uri":{"scheme":"https","host":"01.pilosa.local","port":10501},"isCoordinator":true}]}
```

The `-k` flag is used to tell curl that it shouldn't bother checking the certificate the server provides, and the `--ipv4` flag avoids an issue on MacOS where the curl request takes a long time if the address resolves to `127.0.0.1`. You can leave it out on Linux and WSL.

If everything is set up correctly, the cluster state should be `NORMAL`.

#### Running Queries

Having confirmed that our cluster is running normally, let's perform a few queries. First, we need to create an index and a field:
``` request
curl https://01.pilosa.local:10501/index/sample-index \
     -k --ipv4 \
     -X POST
```
``` response
{"success":true}
```

This will create index `sample-index` with default options. Let's create the field now:
``` request
curl https://01.pilosa.local:10501/index/sample-index/field/sample-field \
     -k --ipv4 \
     -X POST
```
``` response
{"success":true}
```

We just created field `sample-field` with default options.

Let's run a `Set` query:
``` request
curl https://01.pilosa.local:10501/index/sample-index/query \
     -k --ipv4 \
     -X POST \
     -d 'Set(100, sample-field=1)'
```
``` response
{"results":[true]}
```

Confirm that the value was indeed set:
``` request
curl https://01.pilosa.local:10501/index/sample-index/query \
     -k --ipv4 \
     -X POST \
     -d 'Row(sample-field=1)'
```
``` response
{"results":[{"attrs":{},"columns":[100]}]}
```

The same response should be returned when querying other nodes in the cluster:
``` request
curl https://02.pilosa.local:10501/index/sample-index/query \
     -k --ipv4 \
     -X POST \
     -d 'Row(sample-field=1)'
```
``` response
{"results":[{"attrs":{},"columns":[100]}]}
```

#### What's Next?

Check out our [Administration Guide](https://www.pilosa.com/docs/latest/administration/) to learn more about making the most of your Pilosa cluster and [Configuration Documentation](https://www.pilosa.com/docs/latest/configuration/) to see the available options to configure Pilosa.

### Setting Up a Docker Cluster

In this tutorial, we will be setting up a 2-node Pilosa cluster using Docker containers.

#### Running a Docker Cluster on a Single Server

The instructions below require Docker 1.13 or better.

Let's first be sure that the Pilosa image is up to date:
```
docker pull pilosa/pilosa:latest
```

Then, create a virtual network to attach our containers. We are going to name our network `pilosanet`:

```
docker network create pilosanet
```

Let's run the first Pilosa node and attach it to that virtual network. We set the first node as the cluster coordinator and use its address as the gossip seed. And also set the server address to `pilosa1`:
```
docker run -it --rm --name pilosa1 -p 10101:10101 --network=pilosanet pilosa/pilosa:latest server --bind pilosa1 --cluster.coordinator=true --gossip.seeds=pilosa1:14000
```

Let's run the second Pilosa node and attach it to the virtual network as well. Note that we set the address of the gossip seed to the address of the first node:
```
docker run -it --rm --name pilosa2 -p 10102:10101 --network=pilosanet pilosa/pilosa:latest server --bind pilosa2 --gossip.seeds=pilosa1:14000
```

Let's test that the nodes in the cluster connected with each other:
``` request
curl localhost:10101/status
```
``` response
{"state":"NORMAL","nodes":[{"id":"2e8332d0-1fee-44dd-a359-e0d6ecbcefc1","uri":{"scheme":"http","host":"pilosa1","port":10101},"isCoordinator":true},{"id":"8c0dbcdc-9503-4265-8ad2-ba85a4bb10fa","uri":{"scheme":"http","host":"pilosa2","port":10101},"isCoordinator":false}],"localID":"2e8332d0-1fee-44dd-a359-e0d6ecbcefc1"}
```

And similarly for the second node:
``` request
curl localhost:10102/status
```
``` response
{"state":"NORMAL","nodes":[{"id":"2e8332d0-1fee-44dd-a359-e0d6ecbcefc1","uri":{"scheme":"http","host":"pilosa1","port":10101},"isCoordinator":true},{"id":"8c0dbcdc-9503-4265-8ad2-ba85a4bb10fa","uri":{"scheme":"http","host":"pilosa2","port":10101},"isCoordinator":false}],"localID":"2e8332d0-1fee-44dd-a359-e0d6ecbcefc1"}
```
The corresponding [Docker Compose](https://docs.docker.com/compose/) file is below:

```yaml
version: '2'
services: 
  pilosa1:
    image: pilosa/pilosa:latest
    ports:
      - "10101:10101"
    environment:
      - PILOSA_CLUSTER_COORDINATOR=true
      - PILOSA_GOSSIP_SEEDS=pilosa1:14000
    networks:
      - pilosanet
    entrypoint:
      - /pilosa
      - server
      - --bind
      - "pilosa1:10101"
  pilosa2:
    image: pilosa/pilosa:latest
    ports:
      - "10102:10101"
    environment:
      - PILOSA_GOSSIP_SEEDS=pilosa1:14000
    networks:
      - pilosanet
    entrypoint:
      - /pilosa
      - server
      - --bind
      - "pilosa2:10101"
networks: 
  pilosanet:
```

#### Running a Docker Swarm

It is very easy to run a Pilosa Cluster on different servers using [Docker Swarm mode](https://docs.docker.com/engine/swarm/). All we have to do is create an overlay network instead of a bridge network.

The instructions in this section require Docker 17.06 or newer. Although it is possible to run a Docker swarm on MacOS or Windows, it is easiest to run it on Linux. The following instructions assume you are running on Linux.

We are going to use two servers: the manager node runs in the first server and a worker node in the second server.

Docker nodes require some ports to be accesible from the outside. Before proceeding, make sure the following ports are open on all nodes: TCP/2377, TCP/7946, UDP/7946, UDP/4789.

Let's initialize the swarm first. Run the following on the manager:
```
docker swarm init --advertise-addr=IP-ADDRESS
```

Virtual machines running on the cloud usually have at least two network interfaces: the external interface and the internal interface. Use the IP of the external interface.

The output of the command above should be similar to:
```
To add a manager to this swarm, run the following command:

    docker swarm join --token SOME-TOKEN MANAGER-IP-ADDRESS:2377
```

Let's make the worker node join the manager. Copy/paste the command above in a shell on the worker, replacing the token and IP address with the correct values. You may neeed to add `--advertise-addr=WORKER-EXTERNAL-IP-ADDRESS` parameter if the worker has more than one network interface:
```
docker swarm join --token SOME-TOKEN MANAGER-IP-ADDRESS:2377
```

Run the following on the manager to check that the worker joined to the swarm:
```
docker node ls
```

Which should output:

ID|HOSTNAME|STATUS|AVAILABILITY|MANAGER STATUS|ENGINE VERSION
---|--------|------|------------|--------------|-------------
MANAGER-ID *|swarm1|Ready|Active|Leader|18.05.0-ce|
WORKER-ID|swarm2|Ready|Active||18.05.0-ce|

If you have created the `pilosanet` network before, delete it before carrying on, otherwise skip to the next step:
```
docker network rm pilosanet
```

Let's create the `pilosanet` network, but with `overlay` type this time. We should also make this network attachable in order to be able to attach containers to it. Run the following on the manager:
```
docker network create -d overlay pilosanet --attachable
```

We can now create the Pilosa containers. Let's start the coordinator node first. Run the following on one of the servers:
```
docker run -it --rm --name pilosa1 --network=pilosanet pilosa/pilosa:latest server --bind pilosa1 --cluster.coordinator=true --gossip.seeds=pilosa1:14000
```

And the following on the other server:
```
docker run -it --rm --name pilosa2 --network=pilosanet pilosa/pilosa:latest server --bind pilosa2 --gossip.seeds=pilosa1:14000
```

These were the same commands we used in the previous section except the port mapping! Let's run another container on the same virtual network to read the status from the coordinator:
``` request
docker run -it --rm --network=pilosanet --name shell alpine wget -q -O- pilosa1:10101/status
```
``` response
{"state":"NORMAL","nodes":[{"id":"3e3b0abd-1945-441a-a01f-5a28272972f5","uri":{"scheme":"http","host":"pilosa1","port":10101},"isCoordinator":true},{"id":"71ed27cc-9443-4f41-88fb-1c22f92bf695","uri":{"scheme":"http","host":"pilosa2","port":10101},"isCoordinator":false}],"localID":"3e3b0abd-1945-441a-a01f-5a28272972f5"}
```

You can add additional worker nodes to both the swarm and the Pilosa cluster using the steps above.

#### What's Next?

Check out our [Administration Guide](https://www.pilosa.com/docs/latest/administration/) to learn more about making the most of your Pilosa cluster and [Configuration Documentation](https://www.pilosa.com/docs/latest/configuration/) to see the available options to configure Pilosa.

Refer to the [Docker documentation](https://docs.docker.com) to see your options about running Docker containers. The [Networking with overlay networks](https://docs.docker.com/network/network-tutorial-overlay/) is a detailed overview of the Docket swarm mode and overlay networks.


### Using Integer Field Values

#### Introduction

Pilosa can store integer values associated to the columns in an index, and those values are used to support `Range`, `Min`, `Max`, and `Sum` queries. In this tutorial we will show how to set up integer fields, populate those fields with data, and query the fields. The example index we're going to create will represent fictional patients at a medical facility and various bits of information about those patients.

First, create an index called `patients`:
``` request
curl localhost:10101/index/patients \
     -X POST 
```
``` response
{"success":true}
```

In addition to storing rows of bits, a field can also store integer values. The next steps creates three fields (`age`, `weight`, `tcells`) in the `measurements` field.
``` request
curl localhost:10101/index/patients/field/age \
     -X POST \
     -d '{"options":{"type": "int", "min": 0, "max": 120}}'
```
``` response
{"success":true}
```

``` request
curl localhost:10101/index/patients/field/weight \
     -X POST \
     -d '{"options":{"type": "int", "min": 0, "max": 500}}'
```
``` response
{"success":true}
```

``` request
curl localhost:10101/index/patients/field/tcells \
     -X POST \
     -d '{"options":{"type": "int", "min": 0, "max": 2000}}'
```
``` response
{"success":true}
```

Next, let's populate our fields with data. There are two ways to get data into fields: use the `Set()` PQL function to set fields individually, or use the `pilosa import` command to import many values at once. First, let's set some field data using PQL.

The following queries set the age, weight, and t-cell count for the patient with ID `1` in our system:
``` request
curl localhost:10101/index/patients/query \
     -X POST \
     -d 'Set(1, age=34)'
```
``` response
{"results":[true]}
```

``` request
curl localhost:10101/index/patients/query \
     -X POST \
     -d 'Set(1, weight=128)'
```
``` response
{"results":[true]}
```

``` request
curl localhost:10101/index/patients/query \
     -X POST \
     -d 'Set(1, tcells=1145)'
```
``` response
{"results":[true]}
```

In the case where we need to load a lot of data at once, we can use the `pilosa import` command. This method lets us import data into Pilosa from a CSV file.

Assuming we have a file called `ages.csv` that is structured like this:
```
1,34
2,57
3,19
4,40
5,32
6,71
7,28
8,33
9,63
```
where the first column of the CSV represents the patient `ID` and the second column represents the patient's `age`, then we can import the data into our `age` field by running this command:
```
pilosa import -i patients -f measurements --field age ages.csv
```

Now that we have some data in our index, let's run a few queries to demonstrate how to use that data.

In order to find all patients over the age of 40, then simply run a `Range` query against the `age` field.
``` request
curl localhost:10101/index/patients/query \
     -X POST \
     -d 'Range(age > 40)'
```
``` response
{"results":[{"attrs":{},"columns":[2,6,9]}]}
```

You can find a list of supported range operators in the [Range Query](../query-language/#range-bsi) documentation.

To find the average age of all patients, run a `Sum` query:
``` request
curl localhost:10101/index/patients/query \
     -X POST \
     -d 'Sum(field="age")'
```
``` response
{"results":[{"value":377,"count":9}]}
```
The results you get from the `Sum` query contain the sum of all values as well as the `count` of columns with a value. To get the average you can just divide `value` by `count`.

You can also provide a filter to the `Sum()` function to find the average age of all patients over 40.
``` request
curl localhost:10101/index/patients/query \
     -X POST \
     -d 'Sum(Range(age > 40), field="age")'
```
``` response
{"results":[{"value":191,"count":3}]}
```
Notice in this case that the count is only `3` because of the `age > 40` filter applied to the query.

To find the minimum age of all patients, run a `Min` query:
``` request
curl localhost:10101/index/patients/query \
     -X POST \
     -d 'Min(field="age")'
```
``` response
{"results":[{"value":19,"count":1}]}
```
The results you get from the `Min` query contain the minimum `value` of all values as well as the `count` of columns with that value.

You can also provide a filter to the `Min()` function to find the minimum age of all patients over 40.
``` request
curl localhost:10101/index/patients/query \
     -X POST \
     -d 'Min(Range(age > 40), field="age")'
```
``` response
{"results":[{"value":57,"count":1}]}
```

To find the maximum age of all patients, run a `Max` query:
``` request
curl localhost:10101/index/patients/query \
     -X POST \
     -d 'Max(field="age")'
```
``` response
{"results":[{"value":71,"count":1}]}
```
The results you get from the `Max` query contain the maximum `value` of all values as well as the `count` of columns with that value.

You can also provide a filter to the `Max()` function to find the maximum age of all patients under 40.
``` request
curl localhost:10101/index/patients/query \
     -X POST \
     -d 'Max(Range(age < 40), field="age")'
```
``` response
{"results":[{"value":34,"count":1}]}
```

### Storing Row and Column Attributes

#### Introduction

Pilosa can store arbitrary values associated to any row or column. In Pilosa, these are referred to as `attributes`, and they can be of type `string`, `integer`, `boolean`, or `float`. In this tutorial we will store some attribute data and then run some queries that return that data.

First, create an index called `books` to use for this tutorial:
``` request
curl localhost:10101/index/books \
     -X POST
```
``` response
{"success":true}
```

Next, create a field in the `books` index called `members` which will represent library members who have read books.
``` request
curl localhost:10101/index/books/field/members \
     -X POST \
     -d '{}'
```
``` response
{"success":true}
```

Now, let's add some books to our index.
``` request
curl localhost:10101/index/books/query \
     -X POST \
     -d 'SetColumnAttrs(1, name="To Kill a Mockingbird", year=1960)
         SetColumnAttrs(2, name="No Name in the Street", year=1972)
         SetColumnAttrs(3, name="The Tipping Point", year=2000)
         SetColumnAttrs(4, name="Out Stealing Horses", year=2003)
         SetColumnAttrs(5, name="The Forever War", year=2008)'
```
``` response
{"results":[null,null,null,null,null]}
```

And add some members.
``` request
curl localhost:10101/index/books/query \
     -X POST \
     -d 'SetRowAttrs(members, 10001, fullName="John Smith")
         SetRowAttrs(members, 10002, fullName="Sue Perkins")
         SetRowAttrs(members, 10003, fullName="Jennifer Hawks")
         SetRowAttrs(members, 10004, fullName="Pedro Vazquez")
         SetRowAttrs(members, 10005, fullName="Pat Washington")'
```
``` response
{"results":[null,null,null,null,null]}
```

At this point we can query one of the `member` records by querying that row.
``` request
curl localhost:10101/index/books/query \
     -X POST \
     -d 'Row(members=10002)'
```
``` response
{"results":[{"attrs":{"fullName":"Sue Perkins"},"columns":[]}]}
```

Now let's add some data to the matrix such that each pair represents a member who has read that book.
``` request
curl localhost:10101/index/books/query \
     -X POST \
     -d 'Set(3, members=10001)
         Set(5, members=10001)
         Set(1, members=10002)
         Set(2, members=10002)
         Set(4, members=10002)
         Set(3, members=10003)
         Set(4, members=10004)
         Set(5, members=10004)
         Set(1, members=10005)
         Set(2, members=10005)
         Set(3, members=10005)
         Set(4, members=10005)
         Set(5, members=10005)'
```
``` response
{"results":[true,true,true,true,true,true,true,true,true,true,true,true,true]}
```

Now pull the record for `Sue Perkins` again.
``` request
curl localhost:10101/index/books/query \
     -X POST \
     -d 'Row(members=10002)'
```
``` response
{"results":[{"attrs":{"fullName":"Sue Perkins"},"columns":[1,2,4]}]}
```
Notice that the result set now contains a list of integers in the `columns` attribute. These integers match the column IDs of the books that Sue has read.

In order to retrieve the attribute information that we stored for each book, we need to add a URL parameter `columnAttrs=true` to the query.
``` request
curl localhost:10101/index/books/query?columnAttrs=true \
     -X POST \
     -d 'Row(members=10002)'
```
``` response
{
  "results":[{"attrs":{"fullName":"Sue Perkins"},"columns":[1,2,4]}],
  "columnAttrs":[
    {"id":1,"attrs":{"name":"To Kill a Mockingbird","year":1960}},
    {"id":2,"attrs":{"name":"No Name in the Street","year":1972}},
    {"id":4,"attrs":{"name":"Out Stealing Horses","year":2003}}
  ]
}
```
The `book` attributes are included in the result set at the `columnAttrs` attribute.

Finally, if we want to find out which books were read by both `Sue` and `Pedro`, we just perform an `Intersect` query on those two members:
``` request
curl localhost:10101/index/books/query?columnAttrs=true \
     -X POST \
     -d 'Intersect(Row(members=10002), Row(members=10004))'
```
``` response
{
  "results":[{"attrs":{},"columns":[4]}],
  "columnAttrs":[
    {"id":4,"attrs":{"name":"Out Stealing Horses","year":2003}}
  ]
}
```

Notice that we don't get row attributes on a complex query, but we still get the column attributes—in this case book information.
