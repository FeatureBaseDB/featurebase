#!/bin/bash

# To run script: ./teardownSmokeTest.sh
# requires TF_VAR_gitlab_token env var to be set

cd qa/tf/ci/smoketest
export TF_IN_AUTOMATION=1
terraform destroy -auto-approve
