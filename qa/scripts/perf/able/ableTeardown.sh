#!/bin/bash

# To run script: ./ableTeardown.sh

cd qa/tf/perf/able
export TF_IN_AUTOMATION=1
terraform destroy -auto-approve
