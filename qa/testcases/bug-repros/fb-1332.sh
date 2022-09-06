#!/bin/bash

. ./config.sh

# datagen some timestamps onto DATANODE0 in index fb_1332_test
$DATAGEN -s custom --custom-config=./fb-1332-datagen.yaml --pilosa.index=fb1332 --pilosa.batch-size=100 --pilosa.hosts=$1
if (( $? != 0 )); then
    echo "couldn't datagen"
    exit 1
fi

# take a backup
featurebase backup --host=$1 -o=fb1332.bak --index=fb1332
if (( $? != 0 )); then
    echo "couldn't backup"
    exit 1
fi

# delete that index
if [[ $( authcurl -XDELETE $1/index/fb1332| jq '.error' ) != "null" ]]; then
    echo "couldn't delete index"
    exit 1
fi

# fb restore
featurebase restore --host=$1 -s=fb1332.bak
if (( $? != 0 )); then
    echo "couldn't restore"
    exit 1
fi


# datagen some more
$DATAGEN -s custom --custom-config=./fb-1332-datagen.yaml --pilosa.index=fb1332 --pilosa.batch-size=100 --pilosa.hosts=$1
if (( $? != 0 )); then
    echo "couldn't ingest a second time"
    exit 1
fi
