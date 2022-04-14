#!/bin/bash

# To run script: ./teardown.sh

cd qa/tf/ci/auth-smoke
export TF_IN_AUTOMATION=1
terraform destroy -auto-approve
