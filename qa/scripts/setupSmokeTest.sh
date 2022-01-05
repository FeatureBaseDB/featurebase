#!/bin/bash

# To run script: ./setupSmokeTest.sh
# requires TF_VAR_gitlab_token env var to be set

pushd ./qa/tf/ci/smoketest
export TF_IN_AUTOMATION=1
echo "Running terraform init..."
terraform init -input=false
echo "Running terraform apply..."
terraform apply -input=false -auto-approve
terraform output -json > outputs.json
popd

# get the bastion host
BASTION=$(cat ./qa/tf/ci/smoketest/outputs.json | jq -r '[.ingest_ips][0]["value"][0]')
echo "using bastion ${BASTION}"

NODE=$(cat ./qa/tf/ci/smoketest/outputs.json | jq -r '[.data_node_ips][0]["value"][0]')
echo "using node ${NODE}"

# remember that the nodes will take at least 2 mins to be up and going and finish cloud-init
echo "Waiting for cluster to become available..."
while true
do
  ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${BASTION} "curl -s http://${NODE}:10101/status" 
  if [ $? -eq 0 ]
  then
    break
  fi
  sleep 20
done


# verify featurebase running
echo "Verifying featurebase cluster running..."
ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${BASTION} "curl -s http://${NODE}:10101/status" 
if (( $? != 0 )) 
then 
    echo "Featurebase cluster not running"
    exit 1
fi

echo "Cluster running."


