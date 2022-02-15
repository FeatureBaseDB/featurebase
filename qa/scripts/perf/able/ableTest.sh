#!/bin/bash

# get the first ingest host
INGESTNODE0=$(cat ./qa/tf/gauntlet/able/outputs.json | jq -r '[.ingest_ips][0]["value"][0]')
echo "using INGESTNODE0 ${INGESTNODE0}"

# get the first data host
DATANODE0=$(cat ./qa/tf/gauntlet/able/outputs.json | jq -r '[.data_node_ips][0]["value"][0]')
echo "using DATANODE0 ${DATANODE0}"


echo "Done."