# Contributing to Pilosa

## Reporting a bug

If you have discovered a bug and don't see it in the [github issue tracker][5], [open a new issue][1]

## Submitting a feature request

Feature requests are managed in Github issues. New features typically go through a [Proposal Process][4]
which starts by [opening a new issue][1] that describes the new feature proposal.

## Submitting code changes

Before you start working on new features, you should [open a new issue][1] to let others know what
you're doing before you start working, otherwise you run the risk of duplicating effort. This also
gives others an opportunity to provide input for your feature.

If you want to help but you aren't sure where to start, check out our [github label for low-effort issues][6].

- Fork the [Pilosa repository][2] and then clone your fork:

    ```shell
    git clone git@github.com:<your-name>/pilosa.git
    ```

- Create a local feature branch:

    ```shell
    git checkout -b something-amazing
    ```

- Commit your changes locally using `git add` and `git commit`.

- Make sure that you've written tests for your new feature, and then run the tests:

    ```shell
    make test
    ```

- Verify that your pull request is applied to the latest version of code on github:

    ```shell
    git remote add upstream git@github.com:pilosa/pilosa.git
    git fetch upstream
    git rebase -i upstream/master
    ```

- Push to your fork:

    ```shell
    git push -u <yourfork> something-amazing
    ```

- Submit a [pull request][3]


[1]: https://github.com/pilosa/pilosa/issues/new
[2]: https://github.com/pilosa/pilosa
[3]: https://github.com/pilosa/pilosa/compare/
[4]: https://github.com/pilosa/general/blob/master/proposal.md
[5]: https://github.com/pilosa/pilosa/issues
[6]: https://github.com/pilosa/pilosa/issues?q=is%3Aopen+is%3Aissue+label%3Anewcomer