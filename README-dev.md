
Development Environment
=======================

Install Go versions 1.6.2+ or 1.7 for your platform.

Fork `github.com/pilosa/pilosa` to your own account. The forked repo will be private.

Make sure `$GOPATH` environment variable points to your Go working directory and `$PATH` incudes `$GOPATH/bin`.

Create a directory (note that we use `github.com/pilosa`, NOT `github.com/USER`) and clone your own Pilosa repo:

```sh
mkdir -p ${GOPATH}/src/github.com/pilosa && cd $_
git clone git@github.com:${USER}/pilosa.git
```

`cd` to your pilosa directory:

```sh
cd ${GOPATH}/src/github.com/pilosa/pilosa
```

Install `dep` to manage dependencies:

```sh
go get -u github.com/golang/dep/cmd/dep
```

Install Pilosa command line tools:

```sh
make install
# or:
# dep ensure && go install github.com/pilosa/pilosa/cmd/...
```

Running `pilosa` should now run a Pilosa instance.

In order to sync your fork with upstream Pilosa repo, add an *upstream* to your repo:

```sh
cd ${GOPATH}/src/github.com/pilosa/pilosa
git remote add upstream git@github.com:pilosa/pilosa.git
```

Before starting to work on a task, sync your branch with the upstream:

```sh
git fetch upstream
git checkout master
git merge upstream/master
```

Create a branch for the task:

```sh
git checkout -b a-branch-for-the-task
```

Update the code in the branch, and commit it.

Push it to your own repo:

```sh
git push --set-upstream origin a-branch-for-the-task
```

All left to do is creating a pull request on github.com.
