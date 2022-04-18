#!/bin/bash

ifErr() {
    RESCODE=$?
    if [[ $RESCODE != 0 ]]; then
        echo $1
        exit $RESCODE
    fi
}

/data/datagen_linux_arm64 -s custom --custom-config=./fb-1371-datagen.yaml --pilosa.index=fb1371 --pilosa.batch-size=100 --pilosa.hosts=$1
ifErr "datagenning fb1371"

RES1=$( curl $1/index/fb1371/query -d 'Max(ts)' | jq '.results[0].timestampValue' )
ifErr "getting Max 1"

RES2=$( curl $1/index/fb1371/query -d 'Max(ts)' | jq '.results[0].timestampValue' )
ifErr "getting Max 2"

if [[ $RES1 != $RES2 ]]; then
    echo "different Max after two calls"
    echo "first: $RES1"
    echo "second: $RES2"
    exit 1
fi
