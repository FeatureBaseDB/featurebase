#!/bin/bash


# for --pilosa.hosts
PILOSA_HOSTS=""

DEPLOYED_DATA_IPS=$(cat ./qa/tf/perf/able/outputs.json | jq -r '[.data_node_ips][0]["value"][]')
echo "DEPLOYED_DATA_IPS: {"
echo "${DEPLOYED_DATA_IPS}"
echo "}"

DEPLOYED_DATA_IPS_LEN=`echo "$DEPLOYED_DATA_IPS" | wc -l`

generatePilosaHostsString() {
    IFS=$'\n'
    cnt=0
    for ip in $DEPLOYED_DATA_IPS
    do
        if (($cnt + 1 != $DEPLOYED_DATA_IPS_LEN)) 
        then
            PILOSA_HOSTS="${PILOSA_HOSTS}p${cnt}=$ip:10101,"
        else
            PILOSA_HOSTS="${PILOSA_HOSTS}p${cnt}=$ip:10101"
        fi
        cnt=$((cnt+1))
    done

    echo "PILOSA_HOSTS: ${PILOSA_HOSTS}"
}

generatePilosaHostsString

datagen -s custom --custom-config=./able.yaml --pilosa.index=seg --pilosa.batch-size=1048576 --pilosa.hosts ${PILOSA_HOSTS}