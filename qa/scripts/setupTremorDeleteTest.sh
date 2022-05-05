#!/bin/bash
set -e 
set +x 

# To run script: ./setupTremorDeleteTest.sh
export TF_IN_AUTOMATION=1

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source $SCRIPT_DIR/utilCluster.sh

# path for ebs device
EBS_DEVICE_NAME=nvme

# featurebase architecture
FB_BINARY=featurebase_linux_amd64
echo "Downloading FeatureBase binary"
aws s3 cp s3://molecula-artifact-storage/featurebase/master/_latest/featurebase_linux_amd64 ./ --profile=$TF_VAR_profile

# datagen binary
DATAGEN_BINARY=datagen
echo "Downloading Datagen binary"
aws s3 cp s3://molecula-artifact-storage/idk/master/_latest/idk-linux-amd64/datagen $DATAGEN_BINARY --profile=$TF_VAR_profile

KAFKA_STATIC=molecula-consumer-kafka-static
echo "Downloading Molecular Consumer Kafka Static binary"
aws s3 cp s3://molecula-artifact-storage/idk/master/_latest/idk-linux-amd64/molecula-consumer-kafka-static ./$KAFKA_STATIC --profile=$TF_VAR_profile

# get the first ingest host
INGESTNODE0=$(cat ./qa/tf/ci/deletetest/outputs.json | jq -r '[.ingest_ips][0]["value"][0]')
echo "using INGESTNODE0 ${INGESTNODE0}"

# get the first data host
DATANODE0=$(cat ./qa/tf/ci/deletetest/outputs.json | jq -r '[.data_node_ips][0]["value"][0]')
echo "using DATANODE0 ${DATANODE0}"

DEPLOYED_CLUSTER_PREFIX=$(cat ./qa/tf/ci/deletetest/outputs.json | jq -r '[.cluster_prefix][0]["value"]')
echo "Using DEPLOYED_CLUSTER_PREFIX: ${DEPLOYED_CLUSTER_PREFIX}"

DEPLOYED_CLUSTER_REPLICA_COUNT=$(cat ./qa/tf/ci/deletetest/outputs.json | jq -r '[.fb_cluster_replica_count][0]["value"]')
echo "Using DEPLOYED_CLUSTER_REPLICA_COUNT: ${DEPLOYED_CLUSTER_REPLICA_COUNT}"

DEPLOYED_DATA_IPS=($(cat ./qa/tf/ci/deletetest/outputs.json | jq -r '[.data_node_ips][0]["value"][]'))
echo "DEPLOYED_DATA_IPS: {"
echo "${DEPLOYED_DATA_IPS[@]}"
echo "}"

DEPLOYED_DATA_IPS_LEN=${#DEPLOYED_DATA_IPS[@]}

DEPLOYED_INGEST_IPS=($(cat ./qa/tf/ci/deletetest/outputs.json | jq -r '[.ingest_ips][0]["value"][]'))
echo "DEPLOYED_INGEST_IPS: {"
echo "${DEPLOYED_INGEST_IPS[@]}"
echo "}"

DEPLOYED_INGEST_IPS_LEN=${#DEPLOYED_INGEST_IPS[@]}


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

setupClusterNodes
rm $FB_BINARY

# verify featurebase running
echo "Verifying featurebase cluster running..."
curl -s http://${DATANODE0}:10101/status
if (( $? != 0 )) 
then 
    echo "Featurebase cluster not running"
    exit 1
fi

echo "Cluster running."

# path for ebs device
EBS_DEVICE_NAME=/dev/sdb

# copy datagen to ingest nodes 
setupIngestNode1(){
    IP=$1
    echo "Setting up first ingest node: ${IP}"
    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ${DATAGEN_BINARY} ec2-user@${IP}:
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${IP} "chmod ugo+x /home/ec2-user/${DATAGEN_BINARY}"
    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ./qa/scripts/delete/tremor_keys.yaml ./qa/scripts/delete/kafkaIngest.sh ec2-user@${IP}:
    setupKafkaServer ${IP}
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${IP} "sudo yum install jq htop tmux -y"
}

setupIngestNode2(){
    IP=$1
    echo "Setting up second ingest node: ${IP}"
    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ./qa/scripts/delete/schema.json ./qa/scripts/delete/featurebaseIngest.sh ec2-user@${IP}:
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${IP} "sudo yum install librdkafka -y"
    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ${KAFKA_STATIC} ec2-user@${IP}:
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${IP} "chmod ugo+x /home/ec2-user/${KAFKA_STATIC}"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${IP} "sudo yum install jq htop tmux nc -y"
}

if [[ $DEPLOYED_INGEST_IPS_LEN -ne "2" ]]; then 
    echo "error expected 2 ingest nodes, got $DEPLOYED_INGEST_IPS_LEN"
    exit 1
fi 

setupIngestNode1 ${DEPLOYED_INGEST_IPS[0]}
setupIngestNode2 ${DEPLOYED_INGEST_IPS[1]}