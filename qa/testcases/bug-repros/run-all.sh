#!/usr/bin/env bash

set -eou pipefail

. ./config.sh
export DATAGEN="/data/datagen_linux_arm64 --auth-token ${ADMIN_TOKEN}"
for file in `ls *-test.sh`; do
    echo "running $file";
    sh -x ./$file "$@"
done
