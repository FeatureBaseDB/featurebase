#!/bin/bash

# To run script: ./deploySingleNodeCluster.sh
# requires TF_VAR_gitlab_token env var to be set

echo “$(pwd)”

pushd ./qa/tf/ci/singlenode
export TF_IN_AUTOMATION=1
terraform init -input=false
terraform apply -input=false -auto-approve
popd

# configure Featurebase

# step 1a: get IPs of the cluster



# step 1b: get IPs of the ingest nodes

# step 2: write a featurebase.conf file

# step 3: write featurebase.service

# step 4: start featurebase

# step 5: verify featurebase running