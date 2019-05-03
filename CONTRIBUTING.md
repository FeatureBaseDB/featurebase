# Contributing to Pilosa

The workflow components of these instructions apply to all Pilosa repositories.

## Reporting a bug

If you have discovered a bug and don't see it in the [github issue tracker][5], [open a new issue][1].

## Submitting a feature request

Feature requests are managed in Github issues, organized with [Zenhub](https://www.zenhub.com/), which is publicly available as a browser extension. New features typically go through a [Proposal Process][4]
which starts by [opening a new issue][1] that describes the new feature proposal.

## Making code contributions

Before you start working on new features, you should [open a new issue][1] to let others know what
you're doing, otherwise you run the risk of duplicating effort. This also
gives others an opportunity to provide input for your feature.

If you want to help but you aren't sure where to start, check out our [github label for low-effort issues][6].


### Development Environment

- Ensure you have a recent version of [Go](https://golang.org/doc/install) installed. Pilosa generally supports the current and previous minor versions; check our [CircleCI config file](../master/.circleci/config.yml) for the most up-to-date information.

- Make sure `$GOPATH` environment variable points to your Go working directory and `$PATH` incudes `$GOPATH/bin`, as described [here](https://golang.org/doc/code.html#GOPATH).

- Fork the [Pilosa repository][2] to your own account.

- It will be easier to follow these instructions if you:

    ```sh
    export GH_USERNAME=<your github username>
	```

- Create a directory (note that we use `github.com/pilosa`, NOT `github.com/USER`) and clone Pilosa:

    ```sh
    mkdir -p ${GOPATH}/src/github.com/pilosa && cd $_
    git clone https://github.com/pilosa/pilosa.git
    ```

- `cd` to your pilosa directory:

    ```sh
    cd ${GOPATH}/src/github.com/pilosa/pilosa
    ```

- Install Pilosa command line tools:

    ```sh
    make install
    ```

    Running `pilosa` should now run a Pilosa instance.

- The official Pilosa repository is your "origin" remote in git. Add your fork as your github username

    ```sh
    cd ${GOPATH}/src/github.com/pilosa/pilosa
    git remote add ${GH_USERNAME} git@github.com:${GH_USERNAME}/pilosa.git
    ```

### Makefile

Pilosa includes a Makefile that automates several tasks:

- Install Pilosa:

    ```sh
    make install
    ```

- Install build dependencies:

    ```sh
    make install-build-deps
    ```

- Create the vendor directory:

    ```sh
    make vendor
    ```

- Run the test suite:

    ```sh
    make test
    ```

- View the coverage report:

    ```sh
    make cover-viz
    ```

- Clear the `vendor/` and `build/` directories:

    ```sh
    make clean
    ```

- Create release tarballs:

    ```sh
    make release
    ```

- Regenerate protocol buffer files in `internal/`:

    ```sh
    make generate-protoc
    ```

- Create tagged Docker image:

    ```sh
    make docker
    ```

- Run tests inside Docker container:

    ```sh
    make docker-test
    ```

Additional commands are available in the `Makefile`.

### Submitting code changes

- Before starting to work on a task, sync your branch with the upstream:

    ```sh
    git checkout master
    git pull
    ```

- Create a local feature branch:

    ```sh
    git checkout -b something-amazing
    ```

- Commit your changes locally using `git add` and `git commit`. Please use [appropriate commit messages](https://chris.beams.io/posts/git-commit/).

- Make sure that you've written tests for your new feature, and then run the tests:

    ```sh
    make test
    ```

- Verify that your pull request is applied to the latest version of code on github:

    ```sh
    git checkout master
    git pull
    git checkout something-amazing
    git rebase master
    ```

- Push to your fork:

    ```sh
    git push -u $GH_USERNAME something-amazing:something-amazing
    ```

- Submit a [pull request][3]


[1]: https://github.com/pilosa/pilosa/issues/new
[2]: https://github.com/pilosa/pilosa
[3]: https://github.com/pilosa/pilosa/compare/
[4]: https://github.com/pilosa/general/blob/master/proposal.md
[5]: https://github.com/pilosa/pilosa/issues
[6]: https://github.com/pilosa/pilosa/issues?q=is%3Aopen+is%3Aissue+label%3Anewcomer
