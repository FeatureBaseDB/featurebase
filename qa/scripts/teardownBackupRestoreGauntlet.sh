#!/bin/bash

# To run script: ./teardownBackupRestoreGauntlet.sh

cd qa/tf/gauntlet/backuprestore
export TF_IN_AUTOMATION=1
terraform destroy -auto-approve
