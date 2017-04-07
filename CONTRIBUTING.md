# Contributing to Pilosa

## Reporting a bug

If you have discovered a bug, [open a new issue][1]

## Submitting a feature request

Feature requests are managed in Github issues. New features typically go through a [Proposal Process][4]
which starts by [opening a new issue][1] that describes the new feature proposal.

## Submitting code changes

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

- Verify that your pull request is applied to the latest version of code on `origin/master`: 

    ```shell
    git fetch origin
    git rebase -i --exec "make test" origin/master
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
