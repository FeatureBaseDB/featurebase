#!/bin/bash

# To run script: ./deleteTeardown.sh

cd qa/tf/perf/delete
export TF_IN_AUTOMATION=1
terraform destroy -auto-approve
