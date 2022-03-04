#!/bin/bash

# To run script: ./setupSmokeTest.sh
export TF_IN_AUTOMATION=1

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source $SCRIPT_DIR/utilCluster.sh

pushd ./qa/tf/ci/smoketest
echo "Running terraform init..."
terraform init -input=false
echo "Running terraform apply..."
terraform apply -input=false -auto-approve
terraform output -json > outputs.json
popd

# get the first ingest host
INGESTNODE0=$(cat ./qa/tf/ci/smoketest/outputs.json | jq -r '[.ingest_ips][0]["value"][0]')
echo "using INGESTNODE0 ${INGESTNODE0}"

# get the first data host
DATANODE0=$(cat ./qa/tf/ci/smoketest/outputs.json | jq -r '[.data_node_ips][0]["value"][0]')
echo "using DATANODE0 ${DATANODE0}"

DEPLOYED_CLUSTER_PREFIX=$(cat ./qa/tf/ci/smoketest/outputs.json | jq -r '[.cluster_prefix][0]["value"]')
echo "Using DEPLOYED_CLUSTER_PREFIX: ${DEPLOYED_CLUSTER_PREFIX}"

DEPLOYED_CLUSTER_REPLICA_COUNT=$(cat ./qa/tf/ci/smoketest/outputs.json | jq -r '[.fb_cluster_replica_count][0]["value"]')
echo "Using DEPLOYED_CLUSTER_REPLICA_COUNT: ${DEPLOYED_CLUSTDEPLOYED_CLUSTER_REPLICA_COUNTER_PREFIX}"

DEPLOYED_DATA_IPS=$(cat ./qa/tf/ci/smoketest/outputs.json | jq -r '[.data_node_ips][0]["value"][]')
echo "DEPLOYED_DATA_IPS: {"
echo "${DEPLOYED_DATA_IPS}"
echo "}"

DEPLOYED_DATA_IPS_LEN=`echo "$DEPLOYED_DATA_IPS" | wc -l`

DEPLOYED_INGEST_IPS=$(cat ./qa/tf/ci/smoketest/outputs.json | jq -r '[.ingest_ips][0]["value"][]')
echo "DEPLOYED_INGEST_IPS: {"
echo "${DEPLOYED_INGEST_IPS}"
echo "}"

DEPLOYED_INGEST_IPS_LEN=`echo "$DEPLOYED_INGEST_IPS" | wc -l`


#wait until we can connect to one of the hosts
for i in {0..24}
do 
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no -o ConnectTimeout=10 ec2-user@${DATANODE0} "pwd"
    if [ $? -eq 0 ]
    then
        echo "Cluster is up after ${i} tries." 
        break
    fi
    sleep 10
done

ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no -o ConnectTimeout=10 ec2-user@${DATANODE0} "pwd"
if [ $? -ne 0 ]
then
    echo "Unable to connect to cluster - giving up" 
    exit 1
fi

setupClusterNodes

# verify featurebase running
echo "Verifying featurebase cluster running..."
curl -s http://${DATANODE0}:10101/status
if (( $? != 0 )) 
then 
    echo "Featurebase cluster not running"
    exit 1
fi

echo "Cluster running."


