#!/bin/bash

# To run script: ./teardownTremorDeleteTest.sh

cd qa/tf/ci/deletetest
export TF_IN_AUTOMATION=1
terraform destroy -auto-approve
