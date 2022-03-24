#!/usr/bin/env bash

set -e

for i in {41..44}; do
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    echo "ROUND $i"
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    /data/datagen_linux_arm64 -s texas_health --pilosa.index thr --end-at 1048575 --pilosa.batch-size 1048576 --concurrency 1 --seed=$i --pilosa.hosts=$1
done
