#!/bin/bash

# get the first ingest host
INGESTNODE0=$(cat ./qa/tf/gauntlet/able/outputs.json | jq -r '[.ingest_ips][0]["value"][0]')
echo "using INGESTNODE0 ${INGESTNODE0}"

# get the first data host
DATANODE0=$(cat ./qa/tf/gauntlet/able/outputs.json | jq -r '[.data_node_ips][0]["value"][0]')
echo "using DATANODE0 ${DATANODE0}"


ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "sudo dnf install https://dl.k6.io/rpm/repo.rpm"
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "sudo dnf install k6"

echo "Copying tests to remote"
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/scripts/perf/able/*.js ec2-user@${INGESTNODE0}:/data
if (( $? != 0 )) 
then 
    echo "Copy failed"
    exit 1
fi

# run smoke test
echo "Running smoke test..."
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "k6 run /data/script.js" 
ABLETESTRESULT=$?

if (( $ABLETESTRESULT != 0 )) 
then 
    echo "able perf test complete with failures"
else
    echo "able test complete"
fi

exit $ABLETESTRESULT 