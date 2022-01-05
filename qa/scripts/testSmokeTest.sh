#!/bin/bash

# get the bastion host
BASTION=$(cat ./qa/tf/ci/smoketest/outputs.json | jq -r '[.ingest_ips][0]["value"][0]')
echo "using bastion ${BASTION}"

NODE=$(cat ./qa/tf/ci/smoketest/outputs.json | jq -r '[.data_node_ips][0]["value"][0]')
echo "using node ${NODE}"

echo "Copying tests to remote"
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/testcases/smoketest ec2-user@${BASTION}:/data
if (( $? != 0 )) 
then 
    echo "Copy failed"
    exit 1
fi

# run smoke test
echo "Running smoke test..."
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${BASTION} " pushd /data; pytest --junitxml=report.xml; popd" 
if (( $? != 0 )) 
then 
    echo "Unable to run smoketest"
    exit 1
fi


echo "Smoke test complete"