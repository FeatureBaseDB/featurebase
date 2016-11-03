
Development Environment
=======================

Install Go versions 1.6.2+ or 1.7 for your platform.

Fork `github.com/umbel/pilosa` to your own account. The forked repo will be private.

In a shell, export your work directory as `$GOPATH`:

```sh
export GOPATH=$HOME/Work/pilosa/server
```

Update `$PATH` to get access to Pilosa command line tools:

```sh
export PATH=GOPATH/bin:$PATH
```

Create a directory (note that we use `github.com/umbel`, NOT `github.com/USER`) and clone your own Pilosa repo:

```sh
mkdir -p ${GOPATH}/src/github.com/umbel && cd $_
git clone git@github.com:${USER}/pilosa.git
```

`cd` to your pilosa directory:

```sh
cd ${GOPATH}/src/github.com/umbel/pilosa
```

Install `godep` to manage dependencies:

```sh
go get -u github.com/tools/godep
```

Install Pilosa command line tools:

```sh
go install github.com/umbel/pilosa/...
```

Running `pilosa` should now run a Pilosa instance.

In order to sync your fork with upstream Umbel repo, add an *upstream* to your repo:

```sh
cd ${GOPATH}/src/github.com/umbel/pilosa
git remote add upstream git@github.com:umbel/pilosa.git
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
