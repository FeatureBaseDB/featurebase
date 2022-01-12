#!/bin/bash

# get the first ingest host
INGESTNODE0=$(cat ./qa/tf/gauntlet/samsung/outputs.json | jq -r '[.ingest_ips][0]["value"][0]')
echo "using INGESTNODE0 ${INGESTNODE0}"

# get the first data host
DATANODE0=$(cat ./qa/tf/gauntlet/samsung/outputs.json | jq -r '[.data_node_ips][0]["value"][0]')
echo "using DATANODE0 ${DATANODE0}"

# generate csv files
echo "Building simulacraData..."
GOOS=linux GOARCH=arm64 go build ./qa/simulacraData/...
if (( $? != 0 )) 
then 
    echo "Build failed"
    exit 1
fi
echo "Copying simulacraData..."
scp -i ~/.ssh/gitlab-featurebase-ci.pem simulacraData ec2-user@${INGESTNODE0}:/data
if (( $? != 0 )) 
then 
    echo "Copy failed"
    exit 1
fi

echo "Running simulacraData..."
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem ec2-user@${INGESTNODE0} "cd /data && /data/simulacraData" 
if (( $? != 0 )) 
then 
    echo "Making big files failed"
    exit 1
fi
echo "Running simulacraData done."

# ingest these files the way that samsung does it
echo "Copying testSamsungPayload.sh..."
scp -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/scripts/testSamsungPayload.sh ec2-user@${INGESTNODE0}:
if (( $? != 0 )) 
then 
    echo "Copying testSamsungPayload.sh failed"
    exit 1
fi

echo "Running (1) testSamsungPayload.sh..."
ssh -T -A -i ~/.ssh/gitlab-featurebase-ci.pem -o ServerAliveInterval=30 ec2-user@${INGESTNODE0} "./testSamsungPayload.sh http://${DATANODE0}:10101 1" 
if (( $? != 0 )) 
then 
    echo "Running 1 testSamsungPayload.sh failed"
    exit 1
fi

echo "Running (0) testSamsungPayload.sh..."
ssh -T -A -i ~/.ssh/gitlab-featurebase-ci.pem -o ServerAliveInterval=30 ec2-user@${INGESTNODE0} "./testSamsungPayload.sh http://${DATANODE0}:10101 0" 
if (( $? != 0 )) 
then 
    echo "Running 0 testSamsungPayload.sh failed"
    exit 1
fi

# query workload that runs


echo "Done."