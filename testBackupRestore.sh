#!/bin/bash

set -eux

declare STATUS="NORMAL"
declare TIMEOUT=30
sleep 4
STATUS=$STATUS timeout -s TERM $TIMEOUT bash -c \
    'while [[ ${STATUS_RECEIVED} != ${STATUS} ]];\
    	do STATUS_RECEIVED=$(curl --connect-timeout 1 -s pilosa0:10101/status | jq  -r ".state") && \
        echo "received status: $STATUS_RECEIVED" && \
        sleep 1;\
    done;'
echo "NOW DO STUFF"
datagen --source kitchensink_keyed -e 9999 --pilosa.index sink --pilosa.batch-size 10000 --pilosa.hosts pilosa0:10101
before=$(/featurebase chksum --host pilosa0:10101)
/featurebase backup -o backupdir --host pilosa0:10101
curl -X DELETE -s pilosa0:10101/index/sink
/featurebase restore -s backupdir --host pilosa0:10101
after=$(/featurebase chksum --host pilosa0:10101)
if [ "$before" = "$after" ]; then
	echo "PASS Cluster"
else
	echo "FAIL Single"
	exit 1
fi
/featurebase restore -s backupdir --host pilosax:10101
single=$(/featurebase chksum --host pilosax:10101)
if [ "$before" = "$single" ]; then
	echo "PASS Single"
	exit 0
else
	echo "FAIL Single"
	exit 1
fi

datagen --source texas_health -e 9999 --pilosa.index newsink --pilosa.batch-size 10000 --pilosa.hosts pilosa0:10101
before=$(/featurebase chksum --host pilosa0:10101)
/featurebase backup -o newbackupdir --host pilosa0:10101 --index newsink
curl -X DELETE -s pilosa0:10101/index/newsink
/featurebase restore -s newbackupdir --host pilosa0:10101
after=$(/featurebase chksum --host pilosa0:10101)
if [ "$before" = "$after" ]; then
	echo "PASS Cluster Table"
else
	echo "FAIL Single Table"
	exit 1
fi
/featurebase restore -s newbackupdir --host pilosax:10101
single=$(/featurebase chksum --host pilosax:10101)
if [ "$before" = "$single" ]; then
	echo "PASS Single Table"
	exit 0
else
	echo "FAIL Single Table"
	exit 1
fi
