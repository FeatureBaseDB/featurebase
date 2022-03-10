#!/bin/bash

# To run script: ./teardownSmokeTest.sh

cd qa/tf/ci/smoketest
export TF_IN_AUTOMATION=1
terraform destroy -auto-approve
