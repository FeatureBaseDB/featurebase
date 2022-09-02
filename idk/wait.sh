#!/bin/sh

name=$1
shift

_start_ts=$(date +%s)
elapsed=0
timeout=120
while :
do
    $@ > /dev/null
    _ret=$?
    _end_ts=$(date +%s)
    if [ $_ret -eq 0 ]; then
        echo "$name is available after $((_end_ts - _start_ts)) seconds."
        break
    else
        echo "Waiting for $name after $((_end_ts - _start_ts)) seconds."
    fi
    sleep 1s
    elapsed=$((elapsed+1))
    if [ $elapsed -ge $timeout ]; then
	exit 110
    fi
done
set -ex
