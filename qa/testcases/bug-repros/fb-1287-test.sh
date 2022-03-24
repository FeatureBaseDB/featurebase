#!/usr/bin/env bash

/data/datagen_linux_arm64 -s custom --custom-config=./fb-1287-datagen.yaml --pilosa.index=fb1287 --pilosa.batch-size=100 --pilosa.hosts=$1

# if we get an error, exit 1
if [[ $( curl $1/index/fb1287/query -d 'Rows(segid,from="2022-01-02T15:04",to="2022-04-02T15:04")' | jq '.error' ) != "null" ]]; then
    exit 1;
else
    exit 0;
fi
