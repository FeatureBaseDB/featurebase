#!/bin/bash

# before running make sure runQueries.sh script is in the same directory as runTremorDeleteTest.sh
# and ./runProducer.sh is copied to the producer node, ./runConsumer.sh copied to the consumer node
# assumes setupTremorDeleteTestCFT.sh has been run to set up data nodes in the cluster and producer/consumer nodes 
# To run script: ./runTremorDeleteTest.sh <IP for data node> <IP for consumer node> <IP for producer node>

INGEST_COUNT=1000000000 
MAX_MSGS=5000000000
BATCH_SIZE=500000
CONCURRENCY=128
PARTITIONS=128
INDEX=tremor
TOPIC=tremor
TEST_NAME="tremor-delete-ci"

producer(){
    SEED=$1 

    # run kafka ingest in the first ingest node - kafka was installed and started in this node 
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${PRODUCER0} "bash runProducer.sh ${DATANODE0} ${PRODUCER0} ${INGEST_COUNT} ${BATCH_SIZE} ${PARTITIONS} ${INDEX} ${TOPIC} ${TEST_NAME} ${SEED}"
}

consumer(){
    sleep 120 # sleep 2min, allow enough messages to be generated in kafka 

    # run ingest to featurebase in the second ingest node 
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${CONSUMER0} "bash runConsumer.sh ${DATANODE0} ${PRODUCER0} ${MAX_MSGS} ${BATCH_SIZE} ${CONCURRENCY} ${INDEX} ${TOPIC} ${TEST_NAME}"

}

ingest(){
    # many producers - produce messages to kafka     
    for i in {1..5}
    do
        echo "launching producer with seed $i"
        ( producer $i & )
    done

    # 1 consumer  - consume messages from kafka to featurebase
    echo "launching consumer" 
    consumer & 
}

runQueries() {
    echo "running queries" 
    # run ingest to featurebase in the second ingest node 
    ssh -A -o StrictHostKeyChecking=no ec2-user@${DATANODE0} "bash runQueries.sh ${DATANODE0} ${INDEX} ${TEST_NAME} ${DEPLOYED_DATA_IPS[@]}"
}

runIngestAndQueries() {
    DATANODE0=$1
    PRODUCER0=$2 # producer node is expected to have kafka running
    CONSUMER0=$3 
    DEPLOYED_DATA_IPS=("${@:4}")

    echo "Producer Node: ${PRODUCER0}, Consumer Node: ${CONSUMER0}, Data Node: ${DATANODE0}"
    echo "DEPLOYED_DATA_IPS: ${DEPLOYED_DATA_IPS[@]}"

    # perform ingest 
    ingest
    
    # wait for all background processes to finish
    for job in `jobs -p`
    do
        wait $job 
    done

    # perform queries 
    runQueries
}