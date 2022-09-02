#!/bin/bash

HOSTS=($@)
/data/datagen --source custom --custom-config /data/backup_test_datagen.yaml --pilosa.index=backup_test --pilosa.hosts=$HOSTS --pilosa.batch-size=1000
# make sure backup doesn't fail
if (( $? != 0 )); then
    echo "datagen failed"
    exit 1
fi

# kill a node somehow
echo "hosts we've got"
for host in ${HOSTS[@]}; do
    echo $host;
done

KILLNODE=${HOSTS[1]}
echo "getting checksum from $KILLNODE"
firstCheckSum=$(featurebase chksum --host $KILLNODE)
echo "first check sum: $firstCheckSum"
if (( $? != 0 ))
then
    echo "getting checksum from $KILLNODE failed"
    exit 1
fi

echo "pausing node $KILLNODE"
ssh -A -o "StrictHostKeyChecking no" ec2-user@${KILLNODE} "sudo systemctl stop featurebase"
if (( $? != 0 ))
then
    echo "pausing node $KILLNODE failed"
    exit 1
fi

# backup from ingest node
featurebase backup --host=${HOSTS[2]} --retry-period=0s --output=backupWOOO
# make sure backup doesn't fail
if (( $? != 0 )); then
    echo "backup failed!!!!!!!!!!"
    exit 1
fi

echo "stopping all featurebase nodes"
# kill all featurebase nodes and wipe the data directory
for host in ${HOSTS[@]}; do
    echo "stopping featurebase on ${host}"
    ssh -A -o "StrictHostKeyChecking no" ec2-user@${host} "sudo systemctl stop featurebase && sudo find /data/featurebase/ -mindepth 1 -delete"
    if (( $? != 0 )); then
        echo "emptying and stopping node failed"
        exit 1
    fi
done

echo "starting all featurebase nodes"
# start all featurebase nodes - systemd
for host in ${HOSTS[@]}; do
    echo "starting featurebase on ${host}"
    ssh -A -o "StrictHostKeyChecking no" ec2-user@${host} "sudo systemctl restart featurebase"
    if (( $? != 0 )); then
        echo "restarting featurebase failed"
        exit 1
    fi
done

echo "waiting for featurebase to start"
# wait until we can connect to one of the hosts
for i in {0..24}; do
    echo "checking ${HOSTS[1]}:10101/status"
    curl -v ${HOSTS[1]}:10101/status
    S=$(curl -s ${HOSTS[1]}:10101/status | jq -r ".state");
    if [[ $S == "NORMAL" ]]; then
        echo "hosts up after $i tries";
        break;
    fi
    echo "attempt $i resulted in $S"
    sleep 5
done


if [[ $S != "NORMAL" ]]; then
    echo "couldn't connect, featurebase never stable"
    exit 1
fi

echo "restoring featurebase"
# featurebase restore
featurebase restore -s=backupWOOO --host=${HOSTS[1]}

# make sure it doesn't fail
if (( $? != 0 )); then
    echo "restore failed!!!!"
    exit 1
fi

# make sure it's the same data
echo "getting checksum from $KILLNODE"
secondChkSum=$(featurebase chksum --host $KILLNODE)
echo "second checkSum $secondChkSum"
if (( $? != 0 )); then
    echo "getting checksum from $KILLNODE failed"
    exit 1
fi

if [[ $firstCheckSum == $secondChkSum ]]; then
    echo "checksums match"
    exit 0
else
    echo "first check sum $firstCheckSum"
    echo "second check sum $secondCheckSum"
    echo "check sums don't match"
    exit 1
fi
