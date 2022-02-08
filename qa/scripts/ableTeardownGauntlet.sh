#!/bin/bash

# To run script: ./ableTeardownGauntlet.sh

cd qa/tf/gauntlet/able
export TF_IN_AUTOMATION=1
terraform destroy -auto-approve
