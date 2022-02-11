#!/bin/bash

#path to the featurebase.conf file
CONFIG_FILE_PATH="/etc/featurebase.conf"
#path to the featurebase.service file
SERVICE_FILE_PATH="/etc/systemd/system/featurebase.service"

#cluster prefix that was used
DEPLOYED_CLUSTER_PREFIX=""

#cluster replica count that was used
DEPLOYED_CLUSTER_REPLICA_COUNT=""

#List of deployed IPs for data nodes
DEPLOYED_DATA_IPS=""
DEPLOYED_DATA_IPS_LEN=0

#List of deployed IPs for ingest nodes
DEPLOYED_INGEST_IPS=""
DEPLOYED_INGEST_IPS_LEN=0

#Initial cluster string
INITIAL_CLUSTER=""

writeFeatureBaseNodeServiceFile() {
    echo "Writing featurebase.service file...index: $1, ip:$2"
    NODEIDX=$1
    NODEIP=$2
    cat << EOT > featurebase.service
# Not Ansible managed

[Unit]
Description="Service for FeatureBase"

[Service]
RestartSec=30
Restart=on-failure
EnvironmentFile=
User=molecula
ExecStart=/usr/local/bin/featurebase server -c /etc/featurebase.conf

[Install]
EOT

    #echo "featurebase.service >>"
    #cat featurebase.service
    #echo "featurebase.service <<"

    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" featurebase.service ec2-user@${NODEIP}:
    if (( $? != 0 )) 
    then 
        echo "featurebase.service copy failed"
        exit 1
    fi

    rm -f featurebase.service
    
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mv featurebase.service ${SERVICE_FILE_PATH}"
}

writeFeatureBaseNodeConfigFile() {
    echo "Writing featurebase.conf file...index: $1, ip:$2"
    NODEIDX=$1
    NODEIP=$2
    cat << EOT > featurebase.conf
name = "p${NODEIDX}"
bind = "0.0.0.0:10101"
bind-grpc = "0.0.0.0:20101"

data-dir = "/data/featurebase"
log-path = "/var/log/molecula/featurebase.log"

max-file-count=900000
max-map-count=900000

long-query-time = "10s"

[postgres]

    bind = "localhost:55432"

[cluster]

    name = "${DEPLOYED_CLUSTER_PREFIX}"
    replicas = ${DEPLOYED_CLUSTER_REPLICA_COUNT}

[etcd]

    listen-client-address = "http://${NODEIP}:10401"
    listen-peer-address = "http://${NODEIP}:10301"
    initial-cluster = "${INITIAL_CLUSTER}"

[metric]

    service = "prometheus"
EOT

    #echo "featurebase.conf >>"
    #cat featurebase.conf
    #echo "featurebase.conf <<"

    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" featurebase.conf ec2-user@${NODEIP}:
    if (( $? != 0 )) 
    then 
        echo "featurebase.conf copy failed"
        exit 1
    fi
    rm -f featurebase.conf
    
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mv featurebase.conf ${CONFIG_FILE_PATH}"
}

executeGeneralNodeConfigCommands() {
    echo "Executing node config...index: $1, ip:$2"
    NODEIDX=$1
    NODEIP=$2

    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mkdir /data"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mkfs.ext4 /dev/nvme1n1"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mount /dev/nvme1n1 /data"

    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo adduser molecula"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mkdir /var/log/molecula"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo chown molecula /var/log/molecula"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mkdir -p /data/featurebase"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo chown molecula /data/featurebase"
    
    # TODO handle different archs
    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" featurebase_linux_arm64 ec2-user@${NODEIP}:
    if (( $? != 0 )) 
    then 
        echo "featurebase binary copy failed"
        exit 1
    fi

    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "chown ec2-user:ec2-user /home/ec2-user/featurebase_linux_arm64"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "chmod ugo+x /home/ec2-user/featurebase_linux_arm64"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mv /home/ec2-user/featurebase_linux_arm64 /usr/local/bin/featurebase"
    
    echo "featurebase binary copied."
}

executeDataStartCommands() {
    echo "executeDataStartCommands...index: $1, ip:$2"
    NODEIDX=$1
    NODEIP=$2

    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo systemctl daemon-reload"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo systemctl start featurebase"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo systemctl enable featurebase"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo systemctl status featurebase"
}

startDataNodes() {
    #now go thru loop again to start up each node
    cnt=0
    for ip in $DEPLOYED_DATA_IPS
    do
        executeDataStartCommands $cnt $ip
        cnt=$((cnt+1))
    done
}

setupDataNode() {
    echo "setting up node $1 at $2"

    writeFeatureBaseNodeConfigFile $1 $2
    writeFeatureBaseNodeServiceFile $1 $2
    executeGeneralNodeConfigCommands $1 $2
}

setupIngestNode() {
    echo "setting up ingest node $1 at $2"
    NODEIDX=$1
    NODEIP=$2

    executeGeneralNodeConfigCommands $1 $2

    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo chown -R ec2-user /data"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "pip3 install -U pytest"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "pip3 install -U requests"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "pip3 install -U json"
}

setupDataNodes() {
    cnt=0
    for ip in $DEPLOYED_DATA_IPS
    do
        setupDataNode $cnt $ip
        cnt=$((cnt+1))
    done
}

setupIngestNodes() {
    cnt=0
    for ip in $DEPLOYED_INGEST_IPS
    do
        setupIngestNode $cnt $ip
        cnt=$((cnt+1))
    done
}

generateInitialClusterString() {
    IFS=$'\n'
    cnt=0
    for ip in $DEPLOYED_DATA_IPS
    do
        if (($cnt + 1 != $DEPLOYED_DATA_IPS_LEN)) 
        then
            INITIAL_CLUSTER="${INITIAL_CLUSTER}p${cnt}=http://$ip:10301,"
        else
            INITIAL_CLUSTER="${INITIAL_CLUSTER}p${cnt}=http://$ip:10301"
        fi
        cnt=$((cnt+1))
    done

    echo "INITIAL_CLUSTER: ${INITIAL_CLUSTER}"
}

setupClusterNodes() {

    #data nodes
    generateInitialClusterString

    setupDataNodes

    startDataNodes

    #ingest nodes
    setupIngestNodes

}