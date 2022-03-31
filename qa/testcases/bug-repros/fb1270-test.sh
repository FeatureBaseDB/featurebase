#!/usr/bin/env bash

set -e

if [ -f /data/datagen_linux_arm64 ]; then
    datagen_loc=/data/datagen_linux_arm64
else
    datagen_loc=`which datagen`
fi

declare -i end=${2:-44} # end at 44 or whatever the second argument is

for (( c=41; c<=$end; c++ )); do
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    echo "ROUND $c"
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    $datagen_loc -s texas_health --pilosa.index thr --end-at 1048575 --pilosa.batch-size 1048576 --concurrency 1 --seed=$c --pilosa.hosts=$1
done
