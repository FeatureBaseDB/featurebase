#!/bin/bash
set -e
set +x

# before running, it is expected that ./setupTremorDeleteTestCFT.sh has been run in a running cluster
# To run script: ./runTremorDeleteTestCFT.sh <10.0.105.100-name> <aws profile>

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

STACK_NAME=$1
AWS_PROFILE=$2

# get the data node ips
DEPLOYED_DATA_IPS=($($SCRIPT_DIR/getASGInstanceIPs.sh ${STACK_NAME}-asg ${AWS_PROFILE}))
echo "DEPLOYED_DATA_IPS: {"
echo "${DEPLOYED_DATA_IPS[@]}"
echo "}"

DATANODE0=${DEPLOYED_DATA_IPS[0]}
echo "using DATANODE0 ${DATANODE0}"

# get producer node IP
PRODUCER_IPS=$($SCRIPT_DIR/getASGInstanceIPs.sh ${STACK_NAME}-producer-asg ${AWS_PROFILE})
PRODUCER0=$(echo $PRODUCER_IPS | head -n1 | cut -d " " -f1)

# get consumer node IP
CONSUMER_IPS=$($SCRIPT_DIR/getASGInstanceIPs.sh ${STACK_NAME}-consumer-asg ${AWS_PROFILE})
CONSUMER0=$(echo $CONSUMER_IPS | head -n1 | cut -d " " -f1)

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source $SCRIPT_DIR/delete/runDeleteTest.sh

# run test
runIngestAndQueries ${DATANODE0} ${PRODUCER0} ${CONSUMER0} "${DEPLOYED_DATA_IPS[@]}"

