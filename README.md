<p>
    <a href="https://www.pilosa.com">
        <img src="https://www.pilosa.com/img/logo.svg" width="50%">
    </a>
</p>


## Our private fork of [Pilosa](https://github.com/pilosa/pilosa), a distributed bitmap index
- [Docs](#docs)
- [Getting Started](#getting-started)
- [Data Model](#data-model)
- [Query Language](#query-language)
- [Client Libraries](#client-libraries)
- [Get Support](#get-support)
- [Contributing](#contributing)

## Docs

See our [internal documentation](https://internal-docs.molecula.cloud), which includes all [external documentation](https://docs.molecula.cloud), plus many internal-only pages, listed under the "Internal" heading in the main navigation bar.

The [documentation](https://www.pilosa.com/docs/) for the open source fork is also available, but is outdated for most technical details. Conceptual explanations and [technical blog posts](https://www.pilosa.com/blog/) may still be informative.

## Getting Started

1.  [Install Pilosa](https://www.pilosa.com/docs/latest/installation/#build-from-source).

Optionally, to include Lattice, the in-browser UI, follow the "Build from source" instructions, and run `make generate-statik` before `make install`. When you run a local Pilosa server on the default host, for example, you can access Lattice at [localhost:10101](http://localhost:10101).

2.  [Start Pilosa](https://www.pilosa.com/docs/getting-started/#starting-pilosa) with the default configuration:

    ```shell
    pilosa server
    ```
    
    and verify that it's running:
    
    ```shell
    curl localhost:10101/status
    ```

3.  Follow along with the [Sample Project](https://internal-docs.molecula.cloud/tutorials/getting-started) to get a better understanding of Pilosa's capabilities.


## Data Model

Check out how the Pilosa [Data Model](https://www.pilosa.com/docs/data-model/) works.


## Query Language

You can interact with Pilosa directly in the console using the [Pilosa Query Language](https://internal-docs.molecula.cloud/explanations/pql-intro) (PQL).


## Client Libraries

Three client libraries were supported for the open source fork: [Go](https://www.pilosa.com/docs/client-libraries/#go), (https://www.pilosa.com/docs/client-libraries/#java), (https://www.pilosa.com/docs/client-libraries/#python). Of these, only the Go client has seen significant use and development in the private fork, and it has now been incorporated into this repository, in [the `client` directory](https://github.com/molecula/pilosa/tree/master/client)

## License

Pilosa is licensed under the Apache License, Version 2.0.

A copy of the license is located in [github.com/molecula/pilosa/LICENSE](https://github.com/pilosa/molecula/blob/master/LICENSE).
More details about licensing are found in [github.com/molecula/pilosa/NOTICE](https://github.com/molecula/pilosa/blob/master/NOTICE).
