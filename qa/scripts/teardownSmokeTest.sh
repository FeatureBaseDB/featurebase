#!/bin/bash

# To run script: ./teardownSmokeTest.sh

# requires TF_VAR_branch env var to be set
if [ -z ${TF_VAR_branch+x} ]; then echo "TF_VAR_branch is unset"; else echo "TF_VAR_branch is set to '$TF_VAR_branch'"; fi

cd qa/tf/ci/smoketest
export TF_IN_AUTOMATION=1
terraform destroy -auto-approve
