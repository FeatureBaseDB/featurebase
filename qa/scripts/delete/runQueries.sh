#!/bin/bash

# To run script: ./runQueries.sh <IP for data node> <index name> <test name for datadog tag>

DATANODE0=""
INDEX=""
TEST_NAME=""

getEpochTime() {
    DATE=$1
    # epoch time since Jan 1, 1970 in days 
    # DATE format is expected to be %Y%M%D
    EPOCH_DATE=$(echo $(( ($(date --date="$DATE" +%s) - $(date --date="700101" +%s)) /(60*60*24))))
}


delete(){
    # get shards for index
    SHARDS=$(curl -s ${DATANODE0}:10101/internal/index/${INDEX}/shards | jq '.shards[]')

    OLDEST_DATE="220102"
    getEpochTime $OLDEST_DATE

    echo "running delete query for data older than ${OLDEST_DATE} - epoch in days: ${EPOCH_DATE}"

    # increment epoch by 1 before creating query string to exclude oldest date 
    ((EPOCH_DATE=$EPOCH_DATE+1))
    
    UNION_STRING=""
    for day in {0..29}
    do 
        CURR_STRING="Row(lastupdated=$EPOCH_DATE)"

        # concatenate row query string 
        UNION_STRING="$UNION_STRING$CURR_STRING,"

        # increment date by 1
        ((EPOCH_DATE=$EPOCH_DATE+1))
    done 

    # perform delete per shard - get all records that have not been updated in the last 30 days
    for SHARD in ${SHARDS}
    do
        SHARD_COUNT=$(curl -s ${DATANODE0}:10101/index/${INDEX}/query -d "Options(Count(Not(Union($UNION_STRING))), shards=[${SHARD}])" | jq -r  '.results[0]')
        if [[ $SHARD_COUNT -gt "0" ]]; then 
            START=$(date +%s%N) # in nano seconds
            RES=$(curl -s ${DATANODE0}:10101/index/${INDEX}/query -d "Options(Delete(Not(Union($UNION_STRING))), shards=[${SHARD}])")
            END=$(date +%s%N) # in nano seconds
            DURATION=$(($(($END-$START))/1000000)) # convert from ns to ms
            echo "deleting records older than ${OLDEST_DATE} from shard ${SHARD}, count: ${SHARD_COUNT}, duration: ${DURATION}ms, result: ${RES}"   
            echo -n "delete_count_per_shard:$SHARD_COUNT|g|#shard:$SHARD,test_name:$TEST_NAME,delete_cycle:$DELETE_CYCLE" | nc -4u -w1 127.0.0.1 8125
            echo -n "delete_duration_per_shard_ms:$DURATION|g|#shard:$SHARD,test_name:$TEST_NAME,delete_cycle:$DELETE_CYCLE" | nc -4u -w1 127.0.0.1 8125
        # commented since it was spamming the logs, only needed for debugging 
        # else
        #     echo "no records found to delete for index $INDEX for query: Options(Count(Not(Union($UNION_STRING))),shards=[${SHARD}])"
        fi 
    done
}

countAll(){
    echo "running countAll query: $i"
    QUERY_STRING=countAll
    START_COUNT=$(date +%s%N)  # in nano seconds
    TOTAL_COUNT=$(curl -s "${DATANODE0}:10101/index/${INDEX}/query" -d "Count(all())" | jq -r  '.results[0]')
    END_COUNT=$(date +%s%N)  # in nano seconds
    DURATION_COUNT=$(($(($END_COUNT-$START_COUNT))/1000000)) # convert from ns to ms

    echo "count for $QUERY_STRING is ${TOTAL_COUNT}, query duration is ${DURATION_COUNT}s"

    echo -n "featurebase_count:$TOTAL_COUNT|g|#test_name:$TEST_NAME,query_string:$QUERY_STRING" | nc -4u -w1 127.0.0.1 8125
    echo -n "featurebase_count_duration_ms:$DURATION_COUNT|g|#test_name:$TEST_NAME,query_string:$QUERY_STRING" | nc -4u -w1 127.0.0.1 8125  
}

countIntersect(){
    echo "running countIntersect query: $i"
    SEGID_1=34999
    SEGID_2=33999
    QUERY_STRING=countIntersect_segid\(${SEGID_1}_${SEGID_2}\)
    START_COUNT=$(date +%s%N)  # in nano seconds
    TOTAL_COUNT=$(curl -s "${DATANODE0}:10101/index/${INDEX}/query" -d"Count(Intersect(Row(segid=$SEGID_1), Row(segid=$SEGID_2)))" | jq -r  '.results[0]')
    END_COUNT=$(date +%s%N)  # in nano seconds
    DURATION_COUNT=$(($(($END_COUNT-$START_COUNT))/1000000)) # convert from ns to ms
    

    echo -n "featurebase_count:$TOTAL_COUNT|g|#test_name:$TEST_NAME,query_string:$QUERY_STRING" | nc -4u -w1 127.0.0.1 8125
    echo -n "featurebase_count_duration_ms:$DURATION_COUNT|g|#test_name:$TEST_NAME,query_string:$QUERY_STRING" | nc -4u -w1 127.0.0.1 8125 
    echo "count for $QUERY_STRING is ${TOTAL_COUNT}, query duration is ${DURATION_COUNT}s"
}

countRowDate(){
    echo "running countRowDate query: $i"
    SEGID=34999
    DATE_FROM=2022-01-15
    DATE_TO=2022-02-02
    QUERY_STRING=countRowDate_segid\(${SEGID}\)_date\(${DATE_FROM}_${DATE_TO}\)
    START_COUNT=$(date +%s%N)  # in nano seconds
    TOTAL_COUNT=$(curl -s "${DATANODE0}:10101/index/${INDEX}/query" -d"Count(Row(segid=$SEGID, from='$DATE_FROM', to='$DATE_TO'))" | jq -r  '.results[0]')
    END_COUNT=$(date +%s%N)  # in nano seconds
    DURATION_COUNT=$(($(($END_COUNT-$START_COUNT))/1000000)) # convert from ns to ms

    echo -n "featurebase_count:$TOTAL_COUNT|g|#test_name:$TEST_NAME,query_string:$QUERY_STRING" | nc -4u -w1 127.0.0.1 8125
    echo -n "featurebase_count_duration_ms:$DURATION_COUNT|g|#test_name:$TEST_NAME,query_string:$QUERY_STRING" | nc -4u -w1 127.0.0.1 8125 
    echo "count for $QUERY_STRING is ${TOTAL_COUNT}, query duration is ${DURATION_COUNT}s"
}

extract(){
    echo "running extract query: $i"
    FIELD=lastupdated
    LIMIT=3
    QUERY_STRING=extract_row\(${FIELD}\)_limit\(${LIMIT}\)
    START_COUNT=$(date +%s%N)  # in nano seconds
    TOTAL_COUNT=$(curl -s "${DATANODE0}:10101/index/${INDEX}/query" -d"Extract(Limit(All( ), limit=$LIMIT), Rows(lastupdated))"| jq -r  '[..|scalars]|length')
    END_COUNT=$(date +%s%N)  # in nano seconds
    DURATION_COUNT=$(($(($END_COUNT-$START_COUNT))/1000000000)) # convert from ns to s

    echo -n "featurebase_extract:$TOTAL_COUNT|g|#test_name:$TEST_NAME,query_string:$QUERY_STRING" | nc -4u -w1 127.0.0.1 8125
    echo -n "featurebase_extract_duration_s:$DURATION_COUNT|g|#test_name:$TEST_NAME,query_string:$QUERY_STRING" | nc -4u -w1 127.0.0.1 8125 
    echo "count for $QUERY_STRING is ${TOTAL_COUNT}, query duration is ${DURATION_COUNT}s"
}

disk_usage() {
    TOTAL_DISK=0
    for IP in ${DEPLOYED_DATA_IPS[@]}
    do
        DISK_PER_NODE=$(curl -s $IP:10101/internal/disk-usage | jq -r '.usage')
        DISK_PER_NODE=$(($DISK_PER_NODE/1000000000)) # conver to GB 
        echo "disk usage is ${DISK_PER_NODE}GB for data node ${IP}"
        TOTAL_DISK=$(($TOTAL_DISK+$DISK_PER_NODE))
    done 
    echo -n "featurebase_disk_usage_GB:$TOTAL_DISK|g|#test_name:$TEST_NAME" | nc -4u -w1 127.0.0.1 8125
    echo "total disk usage for all nodes is ${TOTAL_DISK}GB"
}

memory_usage() {
    TOTAL_MEMORY=0
    for IP in ${DEPLOYED_DATA_IPS[@]}
    do
        MEMORY_PER_NODE=$(curl -s localhost:10101/internal/mem-usage | jq -r '.totalUsed')
        MEMORY_PER_NODE=$(($MEMORY_PER_NODE/1000000000)) # conver to GB 
        echo "memory usage is ${MEMORY_PER_NODE}GB for data node ${IP}"
        TOTAL_MEMORY=$(($TOTAL_MEMORY+$MEMORY_PER_NODE))
    done 
    echo -n "featurebase_memory_usage_GB:$TOTAL_MEMORY|g|#test_name:$TEST_NAME" | nc -4u -w1 127.0.0.1 8125
    echo "total memory usage for all nodes is ${TOTAL_MEMORY}GB"
}

runAllQueries(){
    echo "Start running queries"
    echo "run multiple queries 10 times"
    for i in {1..10}
    do
        countAll 
        countIntersect 
        countRowDate 
        extract 
    done

    # get count, disk and memory prior to delete 
    countAll 
    disk_usage 
    memory_usage 

    # perform delete 
    delete 

    # get count, disk and memory after delete
    countAll
    disk_usage 
    memory_usage   
}

DATANODE0=$1 
INDEX=$2
TEST_NAME=$3
DEPLOYED_DATA_IPS=("${@:4}")

echo "Data node IPs: ${DEPLOYED_DATA_IPS[@]}"

runAllQueries