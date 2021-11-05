#!/usr/bin/env bash

# actually get test coverage for every single package and subpackage
# very slow but oh well what are you gonna do, not test things?  
echo "mode: atomic" > coverage.out
for pkg in $(go list all | grep featurebase); do
    go test -coverprofile=pkgcoverage.out -covermode=atomic $pkg;
    tail -n +2 pkgcoverage.out >> coverage.out; done
