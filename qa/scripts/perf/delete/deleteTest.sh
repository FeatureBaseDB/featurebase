#!/bin/bash

# run ./deleteTest.sh <CI_COMMIT_BRANCH_NAME>

CI_COMMIT_BRANCH=$1

# get the first ingest host
INGESTNODE0=$(cat ./qa/tf/perf/delete/outputs.json | jq -r '[.ingest_ips][0]["value"][0]')
echo "using INGESTNODE0 ${INGESTNODE0}"

# get the first data host
DATANODE0=$(cat ./qa/tf/perf/delete/outputs.json | jq -r '[.data_node_ips][0]["value"][0]')
echo "using DATANODE0 ${DATANODE0}"

# download datagen
aws s3 cp s3://molecula-artifact-storage/idk/${CI_COMMIT_BRANCH}/_latest/idk-linux-arm64/datagen datagen
if (( $? != 0 ))
then
    echo "datagen binary copy failed"
    exit 1
fi

# make it executable
chmod +x datagen
if (( $? != 0 ))
then
    echo "couldn't make datagen executable"
    exit 1
fi

# copy it over to the ingest node
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./datagen ec2-user@${INGESTNODE0}:/data
if (( $? != 0 ))
then
    echo "datagen copy failed"
    exit 1
fi

# setup the yum repo needed for librdkafka onto the ingest node
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/scripts/perf/delete/confluent ec2-user@${INGESTNODE0}:/data
if (( $? != 0 ))
then
    echo "confluent repo setup copy failed"
    exit 1
fi

echo "setting up confluent repo"
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "sudo mv /data/confluent /etc/yum.repos.d"
if (( $? != 0 ))
then
    echo "setting up confluent repo failed"
    exit 1
fi


echo "installing librdkafka on ingest node"
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "sudo rpm --import http://packages.confluent.io/rpm/3.1/archive.key && sudo yum clean all && sudo yum install librdkafka-devel -y"
if (( $? != 0 ))
then
    echo "librdkafka install failed"
    exit 1
fi

# copy tremor.yaml over to the ingest node
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/scripts/perf/delete/tremor.yaml ec2-user@${INGESTNODE0}:/data
if (( $? != 0 ))
then
    echo "tremor.yaml copy failed"
    exit 1
fi

# copy the tests over to ingest node
scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/scripts/perf/delete/test.py ec2-user@${INGESTNODE0}:/data
if (( $? != 0 ))
then
    echo "test copy failed"
    exit 1
fi

# run test on ingest node
echo "running delete test"
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "cd /data; python3 test.py ${DATANODE0}"
TESTRESULT=$?

if (( $TESTRESULT != 0 ))
then
    echo "delete test failed"
else
    echo "delete test complete"
fi

exit $TESTRESULT
