#!/bin/bash

# requires TF_VAR_branch env var to be set
if [ -z ${TF_VAR_branch+x} ]; then echo "TF_VAR_branch is unset"; else echo "TF_VAR_branch is set to '$TF_VAR_branch'"; fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source $SCRIPT_DIR/utilCluster.sh

# get the first ingest host
INGESTNODE0=$(cat ./qa/tf/ci/smoketest/outputs.json | jq -r '[.ingest_ips][0]["value"][0]')
echo "using INGESTNODE0 ${INGESTNODE0}"

# get the first data host
DATANODE0=$(cat ./qa/tf/ci/smoketest/outputs.json | jq -r '[.data_node_ips][0]["value"][0]')
echo "using DATANODE0 ${DATANODE0}"

echo "Writing config.py file..."
cat << EOT > config.py
datanode0="${DATANODE0}"
EOT
mv config.py ./qa/testcases/smoketest/config.py

echo "Copying tests to remote"
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/testcases/smoketest/*.py ec2-user@${INGESTNODE0}:/data
if (( $? != 0 )) 
then 
    echo "Copy failed"
    exit 1
fi

# run smoke test
echo "Running smoke test..."
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "cd /data; ~/.local/bin/pytest --junitxml=report.xml" 
SMOKETESTRESULT=$?

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