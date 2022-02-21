#!/bin/bash

# get the first ingest host
INGESTNODE0=$(cat ./qa/tf/perf/able/outputs.json | jq -r '[.ingest_ips][0]["value"][0]')
echo "using INGESTNODE0 ${INGESTNODE0}"

# get the first data host
DATANODE0=$(cat ./qa/tf/perf/able/outputs.json | jq -r '[.data_node_ips][0]["value"][0]')
echo "using DATANODE0 ${DATANODE0}"


# ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "wget https://github.com/grafana/k6/releases/download/v0.36.0/k6-v0.36.0-linux-arm64.tar.gz"
# ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "tar -xvf k6-v0.36.0-linux-arm64.tar.gz"
# ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "mkdir bin"
# ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "mv ./k6-v0.36.0-linux-arm64/k6 ./bin"

echo "Copying tests to remote"
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/scripts/perf/able/*.js ec2-user@${INGESTNODE0}:/data
if (( $? != 0 )) 
then 
    echo "Copy failed"
    exit 1
fi

# copy restore data to ingest node

# run the restore

# run smoke test
echo "Running perf test..."
#ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "/home/ec2-user/bin/k6 run -e DATANODE0=test.k6.io /data/highcardinalitygroupby.js" 
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "curl ${DATANODE0}:10101/index/seg/query -X POST -o /data/response.json -d 'GroupBy(Rows(education_level), Rows(gender), Rows(political_party), Rows(domain))'" 
ABLETESTRESULT=$?

if (( $ABLETESTRESULT != 0 )) 
then 
    echo "able perf test complete with failures"
else
    echo "able test complete"
fi

exit $ABLETESTRESULT 