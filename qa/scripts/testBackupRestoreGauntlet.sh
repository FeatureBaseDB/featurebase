#!/bin/bash

BRANCH_NAME=$1 

echo "Running tests for branch ${BRANCH_NAME}"

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source $SCRIPT_DIR/utilCluster.sh

# get the first ingest host
INGESTNODE0=$(cat ./qa/tf/gauntlet/backuprestore/outputs.json | jq -r '[.ingest_ips][0]["value"][0]')
echo "using INGESTNODE0 ${INGESTNODE0}"

# get the first data host
DATANODE0=$(cat ./qa/tf/gauntlet/backuprestore/outputs.json | jq -r '[.data_node_ips][0]["value"][0]')
echo "using DATANODE0 ${DATANODE0}"

# get the data node host ips
HOSTS=($( cat ./qa/tf/gauntlet/backuprestore/outputs.json | jq -r '.data_node_ips.value' | tr -d '[],"'))

echo "using hosts:"
for host in ${HOSTS[@]}; do
    echo $host;
done

installDatagen $INGESTNODE0 $BRANCH_NAME

# copy the tests over to ingest node
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/scripts/backupRestoreTest.sh ec2-user@${INGESTNODE0}:/data
if (( $? != 0 ))
then
    echo "test copy failed"
    exit 1
fi

# copy the datagen over to ingest node
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/scripts/backup_test_datagen.yaml ec2-user@${INGESTNODE0}:/data
if (( $? != 0 ))
then
    echo "test copy failed"
    exit 1
fi


echo "running backup test with hosts: ${HOSTS[@]}"
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "cd /data; ./backupRestoreTest.sh ${HOSTS[@]}"
TESTRESULT=$?

if (( $TESTRESULT != 0 ))
then
    echo "backup test failed"
else
    echo "backup test complete"
fi

exit $TESTRESULT
