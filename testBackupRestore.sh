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
before=$(/pilosa chksum --host pilosa0:10101)
/pilosa backup -o backupdir --host pilosa0:10101
curl -X DELETE -s pilosa0:10101/index/sink
/pilosa restore -s backupdir --host pilosa0:10101
after=$(/pilosa chksum --host pilosa0:10101)
if [ "$before" = "$after" ]; then
	echo "PASS Cluster"
else
	echo "FAIL Single"
	exit 1
fi
/pilosa restore -s backupdir --host pilosax:10101
single=$(/pilosa chksum --host pilosax:10101)
if [ "$before" = "$single" ]; then
	echo "PASS Single"
	exit 0
else
	echo "FAIL Single"
	exit 1
fi
