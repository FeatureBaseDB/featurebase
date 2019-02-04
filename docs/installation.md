+++
title = "Installation"
weight = 2
nav = [
    "Installing on MacOS",
    "Installing on Linux",
]
+++


## Installation

Pilosa is currently available for [MacOS](#installing-on-macos) and [Linux](#installing-on-linux).

### Installing on MacOS

There are four ways to install Pilosa on MacOS: Use [Homebrew](https://brew.sh/) (recommended), download the binary, build from source, or use [Docker](#docker).

#### Use Homebrew

1. Update your Homebrew formulas:
    ```
    brew update
    ```

2. Install Pilosa
    ```
    brew install pilosa
    ```

3. Make sure Pilosa is installed successfully:
    ```
    pilosa
    ```

    If you see something like:
    ```
    Pilosa is a fast index to turbocharge your database.

    This binary contains Pilosa itself, as well as common
    tools for administering pilosa, importing/exporting data,
    backing up, and more. Complete documentation is available
    at https://www.pilosa.com/docs/.

    Version: v1.2.0
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

    You're good to go!

#### Download the Binary

1. Download the latest release:
    ```
    curl -L -O https://github.com/pilosa/pilosa/releases/download/v1.2.0/pilosa-v1.2.0-darwin-amd64.tar.gz
    ```

    Other releases can be downloaded from our Releases page on Github.

2. Extract the binary:
    ```
    tar xfz pilosa-v1.2.0-darwin-amd64.tar.gz
    ```

3. Move the binary into your PATH so you can run `pilosa` from any shell:
    ```
    cp -i pilosa-v1.2.0-darwin-amd64/pilosa /usr/local/bin
    ```

4. Make sure Pilosa is installed successfully:
    ```
    pilosa
    ```

    If you see something like:
    ```
    Pilosa is a fast index to turbocharge your database.

    This binary contains Pilosa itself, as well as common
    tools for administering pilosa, importing/exporting data,
    backing up, and more. Complete documentation is available
    at https://www.pilosa.com/docs/.

    Version: v1.2.0
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

    You're good to go!

#### Build from Source

<div class="note">
    <p>For advanced instructions for building from source, view our <a href="https://github.com/pilosa/pilosa/blob/master/CONTRIBUTING.md">Contributor's Guide.</a></p>
</div>

1. Install the prerequisites:

    * [Go](https://golang.org/doc/install). Be sure to set the `$GOPATH` and `$PATH` environment variables as described [here](https://golang.org/doc/code.html#GOPATH).
    * [Git](https://git-scm.com/)

2. Clone the repo:
    ```
    mkdir -p ${GOPATH}/src/github.com/pilosa && cd $_
    git clone https://github.com/pilosa/pilosa.git
    ```

3. Build the Pilosa repo:
    ```
    cd $GOPATH/src/github.com/pilosa/pilosa
    make install-build-deps
    make install
    ```

4. Make sure Pilosa is installed successfully:
    ```
    pilosa
    ```

    If you see something like:
    ```
    Pilosa is a fast index to turbocharge your database.

    This binary contains Pilosa itself, as well as common
    tools for administering pilosa, importing/exporting data,
    backing up, and more. Complete documentation is available
    at https://www.pilosa.com/docs/.

    Version: v1.2.0
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

    You're good to go!

#### What's next?

Head over to the [Getting Started](../getting-started/) guide to create your first Pilosa index.


### Installing on Linux

There are three ways to install Pilosa on Linux: download the binary (recommended), build from source, or use [Docker](#docker).

#### Download the Binary

1. To install the latest version of Pilosa, download the latest release:
    ```
    curl -L -O https://github.com/pilosa/pilosa/releases/download/v1.2.0/pilosa-v1.2.0-linux-amd64.tar.gz
    ```

    Note: This assumes you are using an `amd64` compatible architecture. Other releases can be downloaded from our Releases page on Github.

2. Extract the binary:
    ```
    tar xfz pilosa-v1.2.0-linux-amd64.tar.gz
    ```

3. Move the binary into your PATH so you can run `pilosa` from any shell:
    ```
    cp -i pilosa-v1.2.0-linux-amd64/pilosa /usr/local/bin
    ```

4. Make sure Pilosa is installed successfully:
    ```
    pilosa
    ```

    If you see something like:
    ```
    Pilosa is a fast index to turbocharge your database.

    This binary contains Pilosa itself, as well as common
    tools for administering pilosa, importing/exporting data,
    backing up, and more. Complete documentation is available
    at https://www.pilosa.com/docs/.

    Version: v1.2.0
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

    You're good to go!

#### Build from Source

<div class="note">
    <p>For advanced instructions for building from source, view our <a href="https://github.com/pilosa/pilosa/blob/master/CONTRIBUTING.md">Contributor's Guide.</a></p>
</div>

1. Install the prerequisites:

    * [Go](https://golang.org/doc/install). Be sure to set the `$GOPATH` and `$PATH` environment variables as described [here](https://golang.org/doc/code.html#GOPATH).
    * [Git](https://git-scm.com/)

2. Clone the repo:
    ```
    mkdir -p ${GOPATH}/src/github.com/pilosa && cd $_
    git clone https://github.com/pilosa/pilosa.git
    ```

3. Build the Pilosa repo:
    ```
    cd $GOPATH/src/github.com/pilosa/pilosa
    make install-build-deps
    make install
    ```

4. Make sure Pilosa is installed successfully:
    ```
    pilosa
    ```

    If you see something like:
    ```
    Pilosa is a fast index to turbocharge your database.

    This binary contains Pilosa itself, as well as common
    tools for administering pilosa, importing/exporting data,
    backing up, and more. Complete documentation is available
    at https://www.pilosa.com/docs/.

    Version: v1.2.0
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

    You're good to go!

#### What's next?

Head over to the [Getting Started](../getting-started/) guide to create your first Pilosa index.


### Windows

Windows is currently not supported as a target deployment platform for Pilosa, but developing and running Pilosa is made possible by Docker. See the [Docker](#docker) documentation for using Docker for Windows and Docker Toolbox.

Windows Subsystem for Linux is currently not supported.

### Docker

1. Install Docker for your platform. On Linux, Docker is available via your package manager. On MacOS, you can use Docker for Mac or Docker Toolbox. On Windows, you can use Docker for Windows or Docker Toolbox.

2. **This step is necessary only if you are using Docker Toolbox**, otherwise skip to step 3:

    a. Start the Docker support using `docker-machine start` in a terminal. The environment variables of the terminal should be updated accordingly, run `docker-machine env` to display the necessary commands.

    b. Set up port forwarding in the VirtualBox GUI or on the command line. Guest port should be 10101. For the host port, 10101 is recommended. If the `VBoxManage` command is in your `PATH`, you can use the following command (assuming you use the default VM):

    ```
    VBoxManage modifyvm "default" --natpf1 "pilosa,tcp,,10101,,10101"
    ```

3. Confirm that the Docker daemon is running in the background:
    ```
    docker version
    ```

    If you are getting a "command not found" or similar, check that `docker` command is in your path. If you don't see the server listed, start the Docker application.


4. Pull the official Pilosa image from Docker Hub:

    ```
    docker pull pilosa/pilosa:latest
    ```

5. Make sure Pilosa is installed successfully, and make it accessible:

    ```
    docker run -d --rm --name pilosa -p 10101:10101 pilosa/pilosa:latest server --bind 0.0.0.0:10101
    ```

6. Check that it is accessible from outside the container.

    Run the following in a separate terminal:
    ```
    curl localhost:10101/schema
    ```

    If that returns `{"indexes":null}` or similar, then Pilosa is accessible from outside the container. Otherwise check that you have correctly typed `-p 10101:10101` when running the Pilosa container and the port mappings in VirtualBox is correct (Docker Toolbox only).

7. When you want to terminate the Pilosa container, you can run the following:
    ```
    docker stop pilosa
    ```

#### What's next?

Head over to the [Getting Started](../getting-started/) guide to create your first Pilosa index.
