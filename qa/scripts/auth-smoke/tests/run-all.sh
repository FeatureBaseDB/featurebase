#!/usr/bin/env bash

set -eou pipefail

for file in `ls *-test.sh`; do
    echo "running $file";
    ./$file "$@"
done
