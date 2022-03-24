#!/usr/bin/env bash


SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source $SCRIPT_DIR/utilCluster.sh

# get the first ingest host
INGESTNODE0=$(cat ./qa/tf/ci/smoketest/outputs.json | jq -r '[.ingest_ips][0]["value"][0]')
echo "using INGESTNODE0 ${INGESTNODE0}"

# get the first data host
DATANODE0=$(cat ./qa/tf/ci/smoketest/outputs.json | jq -r '[.data_node_ips][0]["value"][0]')
echo "using DATANODE0 ${DATANODE0}"


# install librdkafka ... workaround until we have static datagen builds for arm
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "sudo yum -y install librdkafka" 

echo "Copying tests to remote"
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/testcases/FB-1270_repro/test.sh ec2-user@${INGESTNODE0}:/data
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./datagen_linux_arm64 ec2-user@${INGESTNODE0}:/data
if (( $? != 0 )) 
then 
    echo "Copy failed"
    exit 1
fi

# run 1270 repro
echo "Running smoke test..."
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "cd /data/; ./test.sh ${DATANODE0}:10101" 
SMOKETESTRESULT=$?


if (( $SMOKETESTRESULT != 0 )) 
then 
    echo "FB-1270 test complete with test failures"
else
    echo "FB-1270 test complete"
fi

exit $SMOKETESTRESULT 
