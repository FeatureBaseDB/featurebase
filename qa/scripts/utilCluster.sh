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

ifErr() {
    res=$?
    if (( res != 0 )); then
        echo "error: $1"
        exit $res
    fi
}

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
    if [[ "$AUTH_ENABLED" = "1" ]]; then
        echo "writing auth enabled featurebase.conf"
        cat << EOT > featurebase.conf
name = "p${NODEIDX}"
bind = "https://0.0.0.0:10101"
bind-grpc = "https://0.0.0.0:20101"

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

[tls]
  certificate = "/data/cert.crt"
  key = "/data/key.key"

[auth]
    enable = true
    client-id = "e9088663-eb08-41d7-8f65-efb5f54bbb71"
    client-secret = "bex7Q~aiWeQ70iBXbEup-XydyQrrNc_Q5n8EW"
    authorize-url = "http://localhost:12345/authorize"
    redirect-base-url = "http://localhost:12345/redirect"
    token-url = "http://localhost:12345/token"
    group-endpoint-url = "http://localhost:12345/groups"
    logout-url = "http://localhost:12345/logout"
    scopes = ["openid", "https://graph.microsoft.com/.default", "offline_access"]
    secret-key = "98995f0530eeba96da1d0a04311073c0abb7b6abbfb0f5f4ef3629527ff88428"
    permissions = "/etc/permissions.yml"
    query-log-path = "/data/featurebase/query.log"
EOT

    echo "writing the permissions file"
    cat << EOT > permissions.yml
"user-groups":
  "group-id-reader":
    "user": "read"
  "group-id-writer":
    "user": "write"
admin: "group-id-admin"
EOT
    else 
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
    fi

    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" featurebase.conf ec2-user@${NODEIP}:
    if (( $? != 0 )) 
    then 
        echo "featurebase.conf copy failed"
        exit 1
    fi
    rm -f featurebase.conf
    
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mv featurebase.conf ${CONFIG_FILE_PATH}"

    if [[ "$AUTH_ENABLED" = "1" ]]; then
        scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" permissions.yml ec2-user@${NODEIP}:
        ifErr "permissions.yml copy failed"
        rm -f permissions.yml

        ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mv ~/permissions.yml /etc/permissions.yml"
        ifErr "mv permissions.yml failed"
    fi
}

setupMkCertCA() {
    # this function sets up a CA on the runner for use by all nodes in setupTLS
    git clone https://github.com/FiloSottile/mkcert && cd mkcert
    ifErr "cloning mkcert"

    go build -ldflags "-X main.Version=$(git describe --tags)"
    ifErr "building mkcert"

    ./mkcert -install
    ifErr "installing root CA"

    cp mkcert /usr/local/bin

    cd ../

}

setupTLS() {
    NODEIP=$1
    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ./qa/scripts/setupTLS.sh ec2-user@${NODEIP}:
    ifErr "setupTLS.sh copy failed"

    scp -r -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ./qa/fakeidp/ ec2-user@${NODEIP}:
    ifErr "fakeidp copy failed"

    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mv ~/fakeidp /etc"
    ifErr "mv fakeidp failed"

    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo ./setupTLS.sh ${NODEIP}"
    ifErr "error setting up TLS"
}

executeGeneralNodeConfigCommands() {
    echo "Executing node config...index: $1, ip:$2"
    NODEIDX=$1
    NODEIP=$2

    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mkdir -p /data"
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
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo yum install git -y"

    echo "featurebase binary copied."

    if [[ "$AUTH_ENABLED" = "1" ]]; then
        caroot=$(mkcert -CAROOT)

        echo "copying root ca to ${NODEIP}"
        scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" "${caroot}/rootCA.pem" ec2-user@${NODEIP}:
        ifErr "copying CAROOT to ${NODEIP}"

        scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" "${caroot}/rootCA-key.pem" ec2-user@${NODEIP}:
        ifErr "copying CAROOT to ${NODEIP}"

        ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mkdir -p /root/.local/share/mkcert && sudo cp ~/rootCA.pem /root/.local/share/mkcert && sudo cp ~/rootCA-key.pem /root/.local/share/mkcert"
        ifErr "cp-ing rootCA.pem to /root/.local/share/mkcert on ${NODEIP}"

        echo "setting up tls certificates"
        setupTLS $NODEIP
    fi
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
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo yum install -y jq"
    installDatagen $NODEIP
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

    if [[ "$AUTH_ENABLED" = "1" ]]; then
        setupMkCertCA
    fi

    setupDataNodes

    startDataNodes

    #ingest nodes
    setupIngestNodes

}

installDatagen() {
    INGESTNODE0=$1
    # download datagen
    aws s3 cp s3://molecula-artifact-storage/idk/master/_latest/idk-linux-arm64/datagen datagen
    if (( $? != 0 ))
    then
        echo "datagen binary copy failed"
        exit 1
    fi

    # make it executable
    chmod +x datagen 
    if (( $? != 0 ))
    then
        echo "couldn't make datagen executable"
        exit 1
    fi

    # copy it over to the ingest node
    scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./datagen ec2-user@${INGESTNODE0}:/data
    if (( $? != 0 )) 
    then 
        echo "datagen copy failed"
        exit 1
    fi

    # setup the yum repo needed for librdkafka onto the ingest node
    scp -r -i ~/.ssh/gitlab-featurebase-ci.pem ./qa/scripts/perf/delete/confluent ec2-user@${INGESTNODE0}:/data
    if (( $? != 0 )) 
    then 
        echo "confluent repo setup copy failed"
        exit 1
    fi

    echo "setting up confluent repo"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "sudo mv /data/confluent /etc/yum.repos.d" 
    if (( $? != 0 ))
    then 
        echo "setting up confluent repo failed"
        exit 1
    fi


    echo "installing librdkafka on ingest node"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ec2-user@${INGESTNODE0} "sudo rpm --import http://packages.confluent.io/rpm/3.1/archive.key && sudo yum clean all && sudo yum install librdkafka-devel -y"
    if (( $? != 0 ))
    then 
        echo "librdkafka install failed"
        exit 1
    fi
}
