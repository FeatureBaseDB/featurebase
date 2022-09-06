#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source $SCRIPT_DIR/utilCluster.sh

# get the first ingest host
must_get_value ./qa/tf/ci/smoketest/outputs.json INGESTNODE0 .ingest_ips 0 '"value"' 0
echo "using INGESTNODE0 ${INGESTNODE0}"

HOSTS=($( cat ./qa/tf/ci/smoketest/outputs.json | jq -r '.data_node_ips.value' | tr -d '[],"'))

echo "Copying tests to remote"
if ! scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/testcases/smoketest/* ./datagen_linux_arm64 ec2-user@${INGESTNODE0}:/data; then
    echo >&2 "Copy failed"
    exit 1
fi

# run smoke test
echo "Running smoke test..."
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "cd /data; ~/.local/bin/pytest --junitxml=report.xml"
SMOKETESTRESULT=$?
if [ $SMOKETESTRESULT -ne 0 ]; then
    echo >&2 "initial smoke tests failed"
    exit 1
fi

echo "Copying test report to local"
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ec2-user@${INGESTNODE0}:/data/report.xml report.xml
if (( $? != 0 ))
then
    echo "Copy failed"
    exit 1
fi

if (( $SMOKETESTRESULT != 0 ))
then
    echo "Smoke test complete with test failures"
else
    echo "Smoke test complete"
fi

exit $SMOKETESTRESULT
