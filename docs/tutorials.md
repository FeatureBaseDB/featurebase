+++
title = "Tutorials"
weight = 4
nav = [
    "Setting Up a Secure Cluster",
    "Using Integer Field Values",
    "Storing Row and Column Attributes",
]
+++

## Tutorials

### Setting Up a Secure Cluster

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


### Using Integer Field Values

#### Introduction

Pilosa can store integer values associated to the columns in an index, and those values are used to support range and aggregate queries. In this tutorial we will show how to set up integer fields, populate those fields with data, and query the fields. The example index we're going to create will represent fictional patients at a medical facility and various bits of information about those patients.

First, create an index called `patients`:
```
curl localhost:10101/index/patients \
     -X POST 
```

Next, create a frame in the `patients` index called `measurements` which will represent information gathered about each patient.
```
curl localhost:10101/index/patients/frame/measurements \
     -X POST \
     -d '{"options":{"rangeEnabled": true}}'
```

In addition to storing rows of bits, a frame can also contain fields that store integer values. The next step creates three fields (`age`, `weight`, `tcells`) in the `measurements` frame.
```
curl localhost:10101/index/patients/frame/measurements \
     -X POST \
     -d '{"options":{
              "rangeEnabled": true,
              "fields": [
                  {"name": "age", "type": "int", "min": 0, "max": 120},
                  {"name": "weight", "type": "int", "min": 0, "max": 500},
                  {"name": "tcells", "type": "int", "min": 0, "max": 2000}
              ]
         }}'
```

If you need to, you can add fields to an existing frame by posting to the [Create Field endpoint](../api-reference/#create-field).

Next, let's populate our fields with data. There are two ways to get data into fields: use the `SetFieldValue()` PQL function to set fields individually, or use the `pilosa import` command to import many values at once. First, let's set some field data using PQL.

This query sets the age, weight, and t-cell count for the patient with ID `1` in our system:
```
curl localhost:10101/index/patients/query \
     -X POST \
     -d 'SetFieldValue(columnID=1, frame="measurements", age=34, weight=128, tcells=1145)'
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
where the first column of the CSV represents the patient `ID` and the second column represents the patient's`age`, then we can import the data into our `age` field by running this command:
```
pilosa import -i patients -f measurements --field age ages.csv
```

Now that we have some data in our index, let's run a few queries to demonstrate how to use that data.

In order to find all patients over the age of 40, then simply run a `Range` query against the `age` field.
```
curl localhost:10101/index/patients/query \
     -X POST \
     -d 'Range(frame="measurements", age > 40)'
```
You should get the following results:
```
{"results":[{"attrs":{},"bits":[2,6,9]}]}
```

You can find a list of supported range operators in the [Range Query](../query-language/#range-bsi) documentation.

To find the average age of all patients, run a `Sum` query:
```
curl localhost:10101/index/patients/query \
     -X POST \
     -d 'Sum(frame="measurements", field="age")'
```
The results you get from the `Sum` query contain the `sum` of all values as well as the `count` of columns with a value. To get the average you can just divide `sum` by `count`.
```
{"results":[{"sum":377,"count":9}]}
```

You can also provide a filter to the `Sum()` function, to find the average age of all patients over 40.
```
curl localhost:10101/index/patients/query \
     -X POST \
     -d 'Sum(Range(frame="measurements", age > 40), frame="measurements", field="age")'
```
Notice in this case that the count is only `3` because of the `age > 40` filter applied to the query.
```
{"results":[{"sum":191,"count":3}]}
```

### Storing Row and Column Attributes

#### Introduction

Pilosa can store arbitrary values associated to any row or column. In Pilosa, these are referred to as `attributes`, and they can be of type `string`, `integer`, `boolean`, or `float`. In this tutorial we will store some attribute data and then run some queries that return that data.

First, create an index called `books` to use for this tutorial:
```
curl localhost:10101/index/books \
     -X POST
```

Next, create a frame in the `books` index called `members` which will represent library members who have read books.
```
curl localhost:10101/index/books/frame/members \
     -X POST \
     -d '{}'
```

Now, let's add some books to our index.
```
curl localhost:10101/index/books/query \
     -X POST \
     -d 'SetColumnAttrs(columnID=1, name="To Kill a Mockingbird", year=1960)
         SetColumnAttrs(columnID=2, name="No Name in the Street", year=1972)
         SetColumnAttrs(columnID=3, name="The Tipping Point", year=2000)
         SetColumnAttrs(columnID=4, name="Out Stealing Horses", year=2003)
         SetColumnAttrs(columnID=5, name="The Forever War", year=2008)'
```

And add some members.
```
curl localhost:10101/index/books/query \
     -X POST \
     -d 'SetRowAttrs(frame="members", rowID=10001, fullName="John Smith")
         SetRowAttrs(frame="members", rowID=10002, fullName="Sue Perkins")
         SetRowAttrs(frame="members", rowID=10003, fullName="Jennifer Hawks")
         SetRowAttrs(frame="members", rowID=10004, fullName="Pedro Vazquez")
         SetRowAttrs(frame="members", rowID=10005, fullName="Pat Washington")'
```

At this point we can query one of the `member` records by querying that row.
```
curl localhost:10101/index/books/query \
     -X POST \
     -d 'Bitmap(frame="members", rowID=10002)'
```
You should get the following result set:
```
{"results":[{"attrs":{"fullName":"Sue Perkins"},"bits":[]}]}
```

Now let's add some data to the matrix such that each pair represents a member who has read that book.
```
curl localhost:10101/index/books/query \
     -X POST \
     -d 'SetBit(frame="members", rowID=10001, columnID=3)
         SetBit(frame="members", rowID=10001, columnID=5)

         SetBit(frame="members", rowID=10002, columnID=1)
         SetBit(frame="members", rowID=10002, columnID=2)
         SetBit(frame="members", rowID=10002, columnID=4)

         SetBit(frame="members", rowID=10003, columnID=3)

         SetBit(frame="members", rowID=10004, columnID=4)
         SetBit(frame="members", rowID=10004, columnID=5)

         SetBit(frame="members", rowID=10005, columnID=1)
         SetBit(frame="members", rowID=10005, columnID=2)
         SetBit(frame="members", rowID=10005, columnID=3)
         SetBit(frame="members", rowID=10005, columnID=4)
         SetBit(frame="members", rowID=10005, columnID=5)'
```

Now pull the record for `Sue Perkins` again.
```
curl localhost:10101/index/books/query \
     -X POST \
     -d 'Bitmap(frame="members", rowID=10002)'
```
Notice that the result set now contains a list of integers in the `bits` attribute. These integers match the column IDs of the books that Sue has read.
```
{"results":[{"attrs":{"fullName":"Sue Perkins"},"bits":[1,2,4]}]}
```

In order to retrieve the attribute information that we stored for each book, we need to add a URL parameter `columnAttrs=true` to the query.
```
curl localhost:10101/index/books/query?columnAttrs=true \
     -X POST \
     -d 'Bitmap(frame="members", rowID=10002)'
```

Here, the `book` attributes will be included in the result set at the `columnAttrs` attribute.

```
{
  "results":[{"attrs":{"fullName":"Sue Perkins"},"bits":[1,2,4]}],
  "columnAttrs":[
    {"id":1,"attrs":{"name":"To Kill a Mockingbird","year":1960}},
    {"id":2,"attrs":{"name":"No Name in the Street","year":1972}},
    {"id":4,"attrs":{"name":"Out Stealing Horses","year":2003}}
  ]
}
```

Finally, if we want to find out which books were read by both `Sue` and `Pedro`, we just perform an `Intersect` query on those two members:
```
curl localhost:10101/index/books/query?columnAttrs=true \
     -X POST \
     -d 'Intersect(Bitmap(frame="members", rowID=10002), Bitmap(frame="members", rowID=10004))'
```

```
{
  "results":[{"attrs":{},"bits":[4]}],
  "columnAttrs":[
    {"id":4,"attrs":{"name":"Out Stealing Horses","year":2003}}
  ]
}
```

Notice that we don't get row attributes on a complex query, but we still get the column attributes—in this case book information.
