#!/bin/bash

# Make sure you have already deployed your cloudformation stack
# before you run this script
# To run script: ./setupTremorDeleteTestCFT.sh <stack-name> <aws profile> <replica count> <fb instance count> <branch-name>

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source $SCRIPT_DIR/utilCluster.sh

# store arguments
STACK_NAME=$1
AWS_PROFILE=$2
DEPLOYED_CLUSTER_REPLICA_COUNT=$3
DEPLOYED_CLUSTER_INSTANCE_COUNT=$4
BRANCH_NAME=$5

echo "Running tests for branch ${BRANCH_NAME}"

# update ebs device name for ingest nodes 
EBS_DEVICE_NAME="/dev/sdb"

# get producer ip (assume only 1 instance for now)
PRODUCER_IPS=$($SCRIPT_DIR/getASGInstanceIPs.sh ${STACK_NAME}-producer-asg ${AWS_PROFILE})
PRODUCER0=$(echo $PRODUCER_IPS | head -n1 | cut -d " " -f1)
echo "setting up producer node, producer_ip: $PRODUCER0, EBS device: $EBS_DEVICE_NAME"
setupProducerNode $PRODUCER0 $BRANCH_NAME

# setup consumer
CONSUMER_IPS=$($SCRIPT_DIR/getASGInstanceIPs.sh ${STACK_NAME}-consumer-asg ${AWS_PROFILE})
CONSUMER0=$(echo $CONSUMER_IPS | head -n1 | cut -d " " -f1)
echo "setting up consumer node, consumer_ip: $CONSUMER0, EBS device: $EBS_DEVICE_NAME"
setupConsumerNode $CONSUMER0 $BRANCH_NAME

# no ebs device, using nvme instead, we can skip ebs mount in executeGeneralNodeConfigCommands()
# nvme is used for data nodes
EBS_DEVICE_NAME="nvme"

# get featurebase binary from the current branch 
FB_BINARY=featurebase_linux_amd64
echo "Downloading FeatureBase binary"
aws s3 cp s3://molecula-artifact-storage/featurebase/${BRANCH_NAME}/_latest/featurebase_linux_amd64 ./ --profile=$AWS_PROFILE

# get the data node ips
DEPLOYED_DATA_IPS=$($SCRIPT_DIR/getASGInstanceIPs.sh ${STACK_NAME}-asg ${AWS_PROFILE})
echo "DEPLOYED_DATA_IPS: {"
echo "${DEPLOYED_DATA_IPS}"
echo "}"
DEPLOYED_DATA_IPS_LEN=`echo "$DEPLOYED_DATA_IPS" | wc -l`

# poll for autoscaling instance counts, until it matches the desired count
ASG_NOT_STABLE="true"
for i in {0..24}
do
    if [ $DEPLOYED_DATA_IPS_LEN -eq $DEPLOYED_CLUSTER_INSTANCE_COUNT ]
    then 
        echo "Autoscaling group is ready after ${i} tries."
        ASG_NOT_STABLE="false"
        break
    fi 
    echo "waiting for autoscaling group to reach steady state..."
    sleep 10
    DEPLOYED_DATA_IPS=$($SCRIPT_DIR/getASGInstanceIPs.sh ${STACK_NAME}-asg ${AWS_PROFILE})
    echo "DEPLOYED_DATA_IPS: {"
    echo "${DEPLOYED_DATA_IPS}"
    echo "}"
    DEPLOYED_DATA_IPS_LEN=`echo "$DEPLOYED_DATA_IPS" | wc -l`    
done

if [[ "$ASG_NOT_STABLE" == "true" ]]
then
    echo "Autoscaling group still unstable - giving up" 
    exit 1
fi

DATANODE0=$(echo $DEPLOYED_DATA_IPS | head -n1 | cut -d " " -f1)
echo "using DATANODE0 ${DATANODE0}"

# define some env var and print them
DEPLOYED_CLUSTER_PREFIX=${STACK_NAME}
echo "Using DEPLOYED_CLUSTER_PREFIX: ${DEPLOYED_CLUSTER_PREFIX}"
echo "Using DEPLOYED_CLUSTER_REPLICA_COUNT: ${DEPLOYED_CLUSTER_REPLICA_COUNT}"

#wait until we can connect to one of the hosts
for i in {0..24}
do 
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no -o ConnectTimeout=10 ec2-user@${DATANODE0} "pwd"
    if [ $? -eq 0 ]
    then
        echo "Cluster is up after ${i} tries." 
        break
    fi
    sleep 10
done

ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no -o ConnectTimeout=10 ec2-user@${DATANODE0} "pwd"
if [ $? -ne 0 ]
then
    echo "Unable to connect to cluster - giving up" 
    exit 1
fi

#setup data nodes
generateInitialClusterString
setupDataNodes $BRANCH_NAME
startDataNodes

# copy script for test 
scp -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ./qa/scripts/delete/runQueries.sh ec2-user@${DATANODE0}:

# verify featurebase running
echo "Verifying featurebase cluster running..."
curl -s http://${DATANODE0}:10101/status
if (( $? != 0 )) 
then 
    echo "Featurebase cluster not running"
    exit 1
fi
echo "FeatureBase Cluster running."
