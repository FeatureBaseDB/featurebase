#!/bin/bash

# To run script: ./setupSamsungGauntlet.sh
export TF_IN_AUTOMATION=1

# requires TF_VAR_gitlab_token env var to be set
if [ -z ${TF_VAR_gitlab_token+x} ]; then echo "TF_VAR_gitlab_token is unset"; else echo "TF_VAR_gitlab_token is set to '$TF_VAR_gitlab_token'"; fi

# requires TF_VAR_branch env var to be set
if [ -z ${TF_VAR_branch+x} ]; then echo "TF_VAR_branch is unset"; else echo "TF_VAR_branch is set to '$TF_VAR_branch'"; fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source $SCRIPT_DIR/utilCluster.sh


pushd ./qa/tf/gauntlet/samsung
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

#wait until we can connect to one of the hosts
for i in {0..24}
do 
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no -o ConnectTimeout=10 ec2-user@${DATANODE0} "pwd"
    if [ $? -eq 0 ]
    then
        echo "Cluster is up after $${i} tries." 
        break
    fi
    sleep 10s
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



