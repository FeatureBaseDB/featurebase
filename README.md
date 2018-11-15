<p>
    <a href="https://www.pilosa.com">
        <img src="https://www.pilosa.com/img/logo.svg" width="50%">
    </a>
</p>

[![CircleCI](https://circleci.com/gh/pilosa/pilosa/tree/master.svg?style=shield)](https://circleci.com/gh/pilosa/pilosa/tree/master)
[![GoDoc](https://godoc.org/github.com/pilosa/pilosa?status.svg)](https://godoc.org/github.com/pilosa/pilosa)
[![Go Report Card](https://goreportcard.com/badge/github.com/pilosa/pilosa)](https://goreportcard.com/report/github.com/pilosa/pilosa)
[![license](https://img.shields.io/github/license/pilosa/pilosa.svg)](https://github.com/pilosa/pilosa/blob/master/LICENSE)
[![CLA Assistant](https://cla-assistant.io/readme/badge/pilosa/pilosa)](https://cla-assistant.io/pilosa/pilosa)
[![GitHub release](https://img.shields.io/github/release/pilosa/pilosa.svg)](https://github.com/pilosa/pilosa/releases)

## An open source, distributed bitmap index.
- [Docs](#docs)
- [Getting Started](#getting-started)
- [Data Model](#data-model)
- [Query Language](#query-language)
- [Client Libraries](#client-libraries)
- [Get Support](#get-support)
- [Contributing](#contributing)

Want to contribute? One of the easiest ways is to [tell us how you're using (or want to use) Pilosa](https://github.com/pilosa/pilosa/issues/1074). We learn from every discussion!

## Docs

See our [Documentation](https://www.pilosa.com/docs/) for information about installing and working with Pilosa.


## Getting Started

1.  [Install Pilosa](https://www.pilosa.com/docs/installation/).

2.  [Start Pilosa](https://www.pilosa.com/docs/getting-started/#starting-pilosa) with the default configuration:

    ```shell
    pilosa server
    ```
    
    and verify that it's running:
    
    ```shell
    curl localhost:10101/nodes
    ```

3.  Follow along with the [Sample Project](https://www.pilosa.com/docs/getting-started/#sample-project) to get a better understanding of Pilosa's capabilities.


## Data Model

Check out how the Pilosa [Data Model](https://www.pilosa.com/docs/data-model/) works.


## Query Language

You can interact with Pilosa directly in the console using the [Pilosa Query Language](https://www.pilosa.com/docs/query-language/) (PQL).


## Client Libraries

There are supported libraries for the following languages:
- [Go](https://www.pilosa.com/docs/client-libraries/#go)
- [Java](https://www.pilosa.com/docs/client-libraries/#java)
- [Python](https://www.pilosa.com/docs/client-libraries/#python)

## Licenses

The core Pilosa code base and all default builds (referred to as Pilosa Community Edition) are licensed completely under the Apache License, Version 2.0.
If you build Pilosa with the `enterprise` build tag (Pilosa Enterprise Edition), then that build will include features licensed under the GNU Affero General
Public License (AGPL). Enterprise code is located entirely in the [github.com/pilosa/pilosa/enterprise](https://github.com/pilosa/pilosa/tree/master/enterprise)
directory. See [github.com/pilosa/pilosa/NOTICE](https://github.com/pilosa/pilosa/blob/master/NOTICE) and
[github.com/pilosa/pilosa/LICENSE](https://github.com/pilosa/pilosa/blob/master/LICENSE) for more information about Pilosa licenses.

## Get Support

There are [several channels](https://www.pilosa.com/community/#support) available for you to reach out to us for support.

## Contributing

Pilosa is an open source project. Please see our [Contributing Guide](CONTRIBUTING.md) for information about how to get involved.
