#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. $SCRIPT_DIR/utilCluster.sh

# get the first ingest host
must_get_value ./qa/tf/ci/smoketest/outputs.json INGESTNODE0 .ingest_ips 0 '"value"' 0
echo "using INGESTNODE0 ${INGESTNODE0}"

# get the first data host
must_get_value ./qa/tf/ci/smoketest/outputs.json DATANODE0 .data_node_ips 0 '"value"' 0
echo "using DATANODE0 ${DATANODE0}"

# install librdkafka ... workaround until we have static datagen builds for arm
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "sudo yum -y install librdkafka"

echo "Copying tests to remote"
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/testcases/bug-repros/ ec2-user@${INGESTNODE0}:/data
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./datagen_linux_arm64 ec2-user@${INGESTNODE0}:/data
if (( $? != 0 ))
then
    echo "Copy failed"
    exit 1
fi

# run all repros
echo "Running bug-repro tests..."
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "cd /data/bug-repros; ./run-all.sh 'https://${DATANODE0}:10101'"
SMOKETESTRESULT=$?


if (( $SMOKETESTRESULT != 0 ))
then
    echo "bug-repro tests complete with test failures"
else
    echo "bug-repro tests complete"
fi

exit $SMOKETESTRESULT
