#!/bin/bash

# get the bastion host
BASTION=$(cat ./qa/tf/ci/smoketest/smoketest.json | jq -r '[.ingest_ips][0]["value"][0]')
echo "using bastion ${BASTION}"

NODE=$(cat ./qa/tf/ci/smoketest/smoketest.json | jq -r '[.data_node_ips][0]["value"][0]')
echo "using node ${NODE}"

echo "Smoke test complete"