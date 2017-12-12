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

There are four ways to install Pilosa on MacOS: Use [Homebrew](https://brew.sh/) (recommended), download the binary, build from source, or use Docker.

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
    at https://www.pilosa.com/docs/

    Version: v0.4.0
    Build Time: 2017-06-08T19:44:21+0000

    Usage:
      pilosa [command]

    Available Commands:
      backup          Backup data from pilosa.
      bench           Benchmark operations.
      check           Do a consistency check on a pilosa data file.
      config          Print the current configuration.
      export          Export data from pilosa.
      generate-config Print the default configuration.
      help            Help about any command
      import          Bulk load data into pilosa.
      inspect         Get stats on a pilosa data file.
      restore         Restore data to pilosa from a backup file.
      server          Run Pilosa.
      sort            Sort import data for optimal import performance.

    Flags:
      -c, --config string   Configuration file to read from.

    Use "pilosa [command] --help" for more information about a command.
    ```

    You're good to go!

#### Download the Binary

1. Download the latest release:
    ```
    curl -L -O https://github.com/pilosa/pilosa/releases/download/v0.8.3/pilosa-v0.8.3-darwin-amd64.tar.gz
    ```

    Other releases can be downloaded from our Releases page on Github.

2. Extract the binary:
    ```
    tar xfz pilosa-v0.8.3-darwin-amd64.tar.gz
    ```

3. Move the binary into your PATH so you can run `pilosa` from any shell:
    ```
    cp -i pilosa-v0.8.3-darwin-amd64/pilosa /usr/local/bin
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
    at https://www.pilosa.com/docs/

    Version: v0.4.0
    Build Time: 2017-06-08T19:44:21+0000

    Usage:
      pilosa [command]

    Available Commands:

      backup          Backup data from pilosa.
      bench           Benchmark operations.
      check           Do a consistency check on a pilosa data file.
      config          Print the current configuration.
      export          Export data from pilosa.
      generate-config Print the default configuration.
      help            Help about any command
      import          Bulk load data into pilosa.
      inspect         Get stats on a pilosa data file.
      restore         Restore data to pilosa from a backup file.
      server          Run Pilosa.
      sort            Sort import data for optimal import performance.

    Flags:
      -c, --config string   Configuration file to read from.

    Use "pilosa [command] --help" for more information about a command.
    ```

    You're good to go!

#### Build from Source

1. Install the prerequisites:

    * [Go](https://golang.org/doc/install). Be sure to set the `$GOPATH` and `$PATH` environment variables as described here (https://golang.org/doc/code.html#GOPATH).
    * [Git](https://git-scm.com/)

2. Clone the repo:
    ```
    go get -d github.com/pilosa/pilosa
    ```

3. Build the Pilosa repo (the `make generate-statik` line isn't necessary but builds a nice web console into Pilosa):
    ```
    cd $GOPATH/src/github.com/pilosa/pilosa
    make generate-statik
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
    at https://www.pilosa.com/docs/

    Version: v0.4.0
    Build Time: 2017-06-08T19:44:21+0000

    Usage:
      pilosa [command]

    Available Commands:
      backup          Backup data from pilosa.
      bench           Benchmark operations.
      check           Do a consistency check on a pilosa data file.
      config          Print the current configuration.
      export          Export data from pilosa.
      generate-config Print the default configuration.
      help            Help about any command
      import          Bulk load data into pilosa.
      inspect         Get stats on a pilosa data file.
      restore         Restore data to pilosa from a backup file.
      server          Run Pilosa.
      sort            Sort import data for optimal import performance.


    Flags:
      -c, --config string   Configuration file to read from.

    Use "pilosa [command] --help" for more information about a command.
    ```

    You're good to go!

#### Use Docker

1. Install Docker for Mac.

2. Confirm that the Docker daemon is running in the background:
    ```
    docker version
    ```

If you don't see the server listed, start the Docker application.

3. Pull the official Pilosa image from Docker Hub:
    ```
    docker pull pilosa/pilosa:latest
    ```

4. Make sure Pilosa is installed successfully:
    ```
    docker run --rm pilosa/pilosa:latest help
    ```

#### What's next?

Head over to the [Getting Started](../getting-started/) guide to create your first Pilosa index.


### Installing on Linux

There are three ways to install Pilosa on Linux: download the binary (recommended), build from source, or use Docker.

#### Download the Binary

1. To install the latest version of Pilosa, download the latest release:
    ```
    curl -L -O https://github.com/pilosa/pilosa/releases/download/v0.8.3/pilosa-v0.8.3-linux-amd64.tar.gz
    ```

    Note: This assumes you are using an `amd64` compatible architecture. Other releases can be downloaded from our Releases page on Github.

2. Extract the binary:
    ```
    tar xfz pilosa-v0.8.3-linux-amd64.tar.gz
    ```

3. Move the binary into your PATH so you can run `pilosa` from any shell:
    ```
    cp -i pilosa-v0.8.3-linux-amd64/pilosa /usr/local/bin
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
    at https://www.pilosa.com/docs/

    Version: v0.4.0
    Build Time: 2017-06-08T19:44:21+0000

    Usage:
      pilosa [command]

    Available Commands:
      backup          Backup data from pilosa.
      bench           Benchmark operations.
      check           Do a consistency check on a pilosa data file.
      config          Print the current configuration.
      export          Export data from pilosa.
      generate-config Print the default configuration.
      help            Help about any command
      import          Bulk load data into pilosa.
      inspect         Get stats on a pilosa data file.
      restore         Restore data to pilosa from a backup file.
      server          Run Pilosa.
      sort            Sort import data for optimal import performance.

    Flags:
      -c, --config string   Configuration file to read from.

    Use "pilosa [command] --help" for more information about a command.
    ```

    You're good to go!

#### Build from Source

1. Install the prerequisites:

    * [Go](https://golang.org/doc/install). Be sure to set the `$GOPATH` and `$PATH` environment variables as described here (https://golang.org/doc/code.html#GOPATH).
    * [Git](https://git-scm.com/)

2. Clone the repo:
    ```
    go get -d github.com/pilosa/pilosa
    ```

3. Build the Pilosa repo:
    ```
    cd $GOPATH/src/github.com/pilosa/pilosa
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
    at https://www.pilosa.com/docs/

    Version: v0.4.0
    Build Time: 2017-06-08T19:44:21+0000

    Usage:
      pilosa [command]

    Available Commands:
      backup          Backup data from pilosa.
      bench           Benchmark operations.
      check           Do a consistency check on a pilosa data file.
      config          Print the current configuration.
      export          Export data from pilosa.
      generate-config Print the default configuration.
      help            Help about any command
      import          Bulk load data into pilosa.
      inspect         Get stats on a pilosa data file.
      restore         Restore data to pilosa from a backup file.
      server          Run Pilosa.
      sort            Sort import data for optimal import performance.

    Flags:
      -c, --config string   Configuration file to read from.

    Use "pilosa [command] --help" for more information about a command.
    ```

    You're good to go!


#### Use Docker

1. Install Docker.

2. Confirm that the Docker daemon is running in the background:
    ```
    docker version
    ```

    If you don't see the server listed, start the Docker application.

3. Pull the official Pilosa image from Docker Hub:
    ```
    docker pull pilosa/pilosa:latest
    ```

4. Make sure Pilosa is installed successfully:
    ```
    docker run --rm pilosa/pilosa:latest help
    ```

#### What's next?

Head over to the [Getting Started](../getting-started/) guide to create your first Pilosa index.


<!--
### Windows

Windows is currently not supported as a target deployment platform for Pilosa, but developing and running Pilosa is made possible by Windows Subsystem for Linux and Docker. See the [Docker](#docker) documentation for using Docker for Windows and Docker Toolbox. You can find documentation about installing Windows Subsystem for Linux at https://msdn.microsoft.com/en-us/commandline/wsl/install_guide. From there, use the instructions in the [Linux Install](#installing-on-linux) section in the this document.

### Docker

1. Install Docker for your platform. On Linux, Docker is available via your package manager. On MacOS, you can use Docker for Mac or Docker Toolbox. On Windows, you can use Docker for Windows or Docker Toolbox.

2. **This step is necessary only if you are using Docker Toolbox**:

    a. Start the Docker support using `docker-machine start` in a terminal. The environment variables of the terminal should be updated accordingly, run `docker-machine env` to display the necessary commands.

    b. Set up port forwarding in the VirtualBox GUI or on the command line. Guest port should be 10101. For the host port, 10101 is recommended. If the `VBoxManage` command is in your `PATH`, you can use the following command (assuming you use the default VM):

    ```
    VBoxManage modifyvm "default" --natpf1 "pilosa,tcp,,10101,,10101"
    ```

3. Confirm that the Docker daemon is running in the background:

    ```
    docker version
    ```

    If you don't see the server listed, start the Docker application.

4. Pull the official Pilosa image from Docker Hub:

    ```
    docker pull pilosa/pilosa:latest
    ```

5. Make sure Pilosa is installed successfully:

    ```
    docker run --rm pilosa/pilosa:latest version
    ```
-->
