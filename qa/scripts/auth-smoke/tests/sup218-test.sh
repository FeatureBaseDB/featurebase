#!/bin/bash

ifErr() {
    res=$?
    if (( res != 0 )); then
        echo "error: $1"
        exit $res
    fi
}

HOSTS=($@)

for host in ${HOSTS[@]}; do
    echo $host;
done

HOST=${HOSTS[2]}

ADMIN_TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiYWRtaW4ifQ.I1iCgk1VU7m6e-En4ACTHIs6V2dZpy_8j2blSSo7K3U"
# ingest string key data to user index
/data/datagen --source custom --custom-config /data/tests/sup218_datagen.yaml --pilosa.index=user --pilosa.hosts=https://$HOST:10101 --pilosa.batch-size=1000 --auth-token=$ADMIN_TOKEN
ifErr "running datagen on $HOST"

# install grpcurl
wget https://github.com/fullstorydev/grpcurl/releases/download/v1.8.6/grpcurl_1.8.6_linux_arm64.tar.gz
tar -xvf grpcurl_1.8.6_linux_arm64.tar.gz
chmod +x grpcurl

for ip in ${HOSTS[@]}; do
    # then make a `select distinct test_field from user`
    ./grpcurl -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiYWRtaW4ifQ.I1iCgk1VU7m6e-En4ACTHIs6V2dZpy_8j2blSSo7K3U' -d '{"sql": "select * from user limit 1"}' $ip:20101 pilosa.Pilosa.QuerySQL
    # make sure it doesn't fail
    ifErr "select * from user failed when it shouldn't have!"
done
