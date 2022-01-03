#!/bin/bash

# get the bastion host
BASTION=$(cat ./qa/tf/gauntlet/samsung/samsung-gauntlet.json | jq -r '[.ingest_ips][0]["value"][0]')
echo "using bastion ${BASTION}"

NODE=$(cat ./qa/tf/gauntlet/samsung/samsung-gauntlet.json | jq -r '[.data_node_ips][0]["value"][0]')
echo "using node ${NODE}"

# generate csv files
GOOS=linux GOARCH=arm64 go build ./qa/simulacraData/...
scp -i ~/.ssh/gitlab-featurebase-ci.pem simulacraData ec2-user@${BASTION}:/data
if (( $? != 0 )) 
then 
    echo "Copy failed"
    exit 1
fi

ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem ec2-user@${BASTION} "cd /data && /data/simulacraData" 
if (( $? != 0 )) 
then 
    echo "Making big files failed"
    exit 1
fi

# ingest these files the way that samsung does it
scp -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/scripts/testSamsungPayload.sh ec2-user@${BASTION}:
if (( $? != 0 )) 
then 
    echo "Copy ingest script failed"
    exit 1
fi

ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem ec2-user@${BASTION} "./testSamsungPayload.sh http://${NODE}:10101 1" 
if (( $? != 0 )) 
then 
    echo "Running 1 testSamsungPayload.sh failed"
    exit 1
fi

ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem ec2-user@${BASTION} "./testSamsungPayload.sh http://${NODE}:10101 0" 
if (( $? != 0 )) 
then 
    echo "Running 0 testSamsungPayload.sh failed"
    exit 1
fi

# query workload that runs