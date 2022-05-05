#!/bin/bash
set -e
set +x

runDatagen(){
    echo "Started datagen on producer node"
    START_DATAGEN=$(date +%s%N) # in nano seconds

    # start datagen - produce messages to kafka server
    ./datagen --kafka.confluent-command.kafka-bootstrap-servers $INGESTNODE0:9092 --source custom --custom-config ./tremor_keys.yaml --pilosa.index $INDEX --pilosa.hosts=$DATANODE --target kafkastatic --kafka.topic $TOPIC --end-at $INGEST_COUNT --kafka.num-partitions $PARTITIONS --track-progress --seed $SEED

    # wait for all background processes to finish
    for job in `jobs -p`
    do
        wait $job 
    done

    END_DATAGEN=$(date +%s%N) # in nano seconds

    # compute duration for ingest
    DURATION_DATAGEN=$(($(($END_DATAGEN-$START_DATAGEN))/1000000000/60)) # convert from ns to min

    # send metrics to datagen 
    echo -n "datagen_duration_min:$DURATION_DATAGEN|g|#test_name:$TEST_NAME" | nc -4u -w1 127.0.0.1 8125   

    echo "finished datagen - duration: $DURATION_DATAGEN min"    
}

DATANODE=$1
shift

INGESTNODE0=$1
shift 

INGEST_COUNT=$1
shift

BATCH_SIZE=$1
shift

PARTITIONS=$1
shift

INDEX=$1
shift

TOPIC=$1
shift 

TEST_NAME=$1
shift 

SEED=$1
shift 

runDatagen