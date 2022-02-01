#!/bin/bash

# To run script: ./teardownSamsungGauntlet.sh

cd qa/tf/gauntlet/samsung
export TF_IN_AUTOMATION=1
terraform destroy -auto-approve
