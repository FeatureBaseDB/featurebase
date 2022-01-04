#!/usr/bin/env bash

# path for featurebase binary
FEATUREBASE_PATH=/usr/local/bin

# path for directory with csv directory files for all fields to be ingested 
CSV_DIR_PATH=/data


# To run:
# ./ingestWorkload.sh {Local host & port for featurebase} {initialize flag}

function delete_field {
    if (($INITIALIZE == 0));
    then 
        curl -XDELETE $HOST/index/$INDEX/field/$FIELD
    fi
}

# Script to replicate samsung workload of deleting and re-ingesting fields every night 
# outline delete and re-ingest workload 
function ingest_int_field { 
    delete_field
    curl -XPOST $HOST/index/$INDEX/field/$FIELD -d '{"options": {"type": "int", "min": 0, "max":'$MAX'}}'
    $FEATUREBASE_PATH/featurebase import --host $HOST -i $INDEX -f $FIELD $CSV_FILE 
}

function ingest_time_field {
    delete_field
    curl -XPOST $HOST/index/$INDEX/field/$FIELD -d '{"options": {"keys": true, "type": "time", "timeQuantum": "YMD"}}'
    $FEATUREBASE_PATH/featurebase import --host $HOST -i $INDEX -f $FIELD $CSV_FILE 
}

function ingest_set_field {
    delete_field
    curl -XPOST $HOST/index/$INDEX/field/$FIELD -d '{"options": {"keys": true}}'
    $FEATUREBASE_PATH/featurebase import --host $HOST -i $INDEX -f $FIELD $CSV_FILE 
}

# featurebase host & port
HOST=$1
shift

# intialize flag - 0:disabled, 1:enabled - creates the index and fields for testing
INITIALIZE=$1
shift 

# get a list of csv files in the directory 
CSV_FILES=`ls $CSV_DIR_PATH/*.csv`

# assign index name 
INDEX="samsung"
if (($INITIALIZE == 1));
then 
    curl -XPOST $HOST/index/$INDEX
fi

# perform delete and re-ingest for all fields 
for CSV_FILE in ${CSV_FILES[@]}
    do 
        # get field name from csv file path 
        FIELD="$(basename $CSV_FILE .csv)"
        if [[ "$FIELD" == *"age"* ]];
        then 
            MAX=100 
            ingest_int_field
        elif [[ "$FIELD" == *"identifier"* ]];
        then 
            MAX=$((2**63 - 1)) # compute max value for 64bit
            ingest_int_field
        elif [[ "$FIELD" == *"ip"* ]];
        then 
            MAX=$((2**31 - 1)) # compute max value for 32bit
            ingest_int_field
        elif [[ "$FIELD" == *"time"* ]];
        then 
            ingest_time_field
        else
            ingest_set_field
        fi
    done



