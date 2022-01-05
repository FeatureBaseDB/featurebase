#!/bin/bash

# To run script: ./teardownSamsungGauntlet.sh
# requires TF_VAR_gitlab_token env var to be set

cd qa/tf/gauntlet/samsung
export TF_IN_AUTOMATION=1
terraform destroy -auto-approve
