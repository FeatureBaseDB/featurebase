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

# ebs device name
EBS_DEVICE_NAME=""

# featurebase binary
FB_BINARY=""

# branch name
BRANCH_NAME=""

# AWS Profile
AWS_PROFILE=""
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

mountEBSVolume() {
    NODEIP=$1
    echo "in mountEBSVolume, EBS_DEVICE_NAME = $EBS_DEVICE_NAME, NODEIP = $NODEIP"
    # if it is nvme, then skip the following 3 lines, because we mount the device in UserData
    # in the CloudFormation template
    if [[ "$EBS_DEVICE_NAME" != "nvme" ]]; then
        echo "mounting EBS device"
        ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mkdir /data"
        ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mkfs.ext4 ${EBS_DEVICE_NAME}"
        ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mount  ${EBS_DEVICE_NAME} /data"
        ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo chown -R ec2-user:ec2-user /data"
    fi  
}

executeGeneralNodeConfigCommands() {
    echo "Executing node config...index: $1, ip:$2"
    NODEIDX=$1
    NODEIP=$2

    mountEBSVolume $NODEIP

    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo adduser molecula"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mkdir /var/log/molecula"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo chown molecula /var/log/molecula"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mkdir -p /data/featurebase"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo chown molecula /data/featurebase"
    
    # TODO handle different archs
    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ${FB_BINARY} ec2-user@${NODEIP}:
    if (( $? != 0 )) 
    then 
        echo "featurebase binary copy failed"
        exit 1
    fi

    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "chown ec2-user:ec2-user /home/ec2-user/${FB_BINARY}"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "chmod ugo+x /home/ec2-user/${FB_BINARY}"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mv /home/ec2-user/${FB_BINARY} /usr/local/bin/featurebase"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo yum install jq htop tmux nc -y"
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
    for ip in ${DEPLOYED_DATA_IPS[@]}
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

    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "pip3 install -U pytest"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "pip3 install -U requests"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "pip3 install -U json"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo yum install -y jq"
    installDatagen $NODEIP
}

setupDataNodes() {
    cnt=0
    for ip in ${DEPLOYED_DATA_IPS[@]}
    do
        setupDataNode $cnt $ip
        setupDatadog $ip "featurebase"
        cnt=$((cnt+1))
    done
}

setupIngestNodes() {
    cnt=0
    echo "setupIngestNodes: " ${DEPLOYED_INGEST_IPS[@]}
    for ip in ${DEPLOYED_INGEST_IPS[@]}
    do
        setupIngestNode $cnt $ip
        setupDatadog $ip "ingest"
        cnt=$((cnt+1))
    done
}

generateInitialClusterString() {
    IFS=$'\n'
    cnt=0
    for ip in ${DEPLOYED_DATA_IPS[@]}
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

writeKafkaServerConfig() {
    echo "Writing server.properties file"
    NODEIP=$1

    cat << EOT > server.properties
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://${NODEIP}:9092,CONTROLLER://:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://${NODEIP}:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/data/kraft-combined-logs
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
group.initial.rebalance.delay.ms=0
confluent.license.topic.replication.factor=1
confluent.metadata.topic.replication.factor=1
confluent.security.event.logger.exporter.kafka.topic.replicas=1
confluent.balancer.enable=true
confluent.balancer.topic.replication.factor=1
EOT

    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" server.properties ec2-user@${NODEIP}:/home/ec2-user/kafka/etc/kafka/kraft/server.properties
    if (( $? != 0 )) 
    then 
        echo "kafka config file: server.properties copy failed"
        exit 1
    fi
    rm -f server.properties
}

setupKafkaServer() {
    NODEIP=$1

    # install kafka 
    echo "setting up kafka server for: $NODEIP"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo yum install java-1.8.0-openjdk nc -y"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "mkdir kafka && cd kafka && curl https://packages.confluent.io/archive/7.0/confluent-community-7.0.1.tar.gz -o kafka.tgz && tar -xvzf kafka.tgz --strip 1"

    # update kafka config 
    writeKafkaServerConfig $NODEIP

cat << 'EOF' >> runKafka.sh 
/home/ec2-user/kafka/bin/kafka-storage format --config /home/ec2-user/kafka/etc/kafka/kraft/server.properties --cluster-id $(/home/ec2-user/kafka/bin/kafka-storage random-uuid)
sudo /home/ec2-user/kafka/bin/kafka-server-start /home/ec2-user/kafka/etc/kafka/kraft/server.properties > /tmp/kafka.log &
echo "checking kafka server status"
EOF

    cat runKafka.sh
    # start zookeeper and kafka server
    echo "starting kafka server"
    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ./runKafka.sh ec2-user@${NODEIP}:.
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "bash runKafka.sh"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "timeout 60s nc -z ${NODEIP} 9092"
    # rm ./runKafka.sh
}

writeDatadogConfigForIngest() {
    # general datadog config 
    cat << EOT > datadog.yaml
api_key: 6ab706d12de9cb46db25a0aabab6a004
site: datadoghq.com
tags:
  - team:core
  - app:${NODE_TYPE}
  - branch_name:${BRANCH_NAME}
process_config:
enabled: "true"
EOT

    # openmetrics config 
cat << EOT > conf.yaml
# The prometheus endpoint to query from
instances:
  - openmetrics_endpoint: "http://localhost:9093/metrics"
    namespace: "ingest"
    metrics:
      - "ingester_kafka_static_.+"
      - go*
EOT
}

writeDatadogConfigForFeaturebase() {
    # general datadog config 
    cat << EOT > datadog.yaml
api_key: 6ab706d12de9cb46db25a0aabab6a004
site: datadoghq.com
tags:
  - team:core
  - app:${NODE_TYPE}
  - branch_name:${BRANCH_NAME}
process_config:
  enabled: "true"
EOT

    # openmetrics config 
cat << EOT > conf.yaml 
# The prometheus endpoint to query from
instances:
  - prometheus_url: http://localhost:10101/metrics
    namespace: "featurebase"
    metrics:
    - prometheus_target_interval_length_seconds: target_interval_length
    - http_requests_total
    - http*
    - etcd*
    - pilosa*
    - go*
    - process*
    - os*
EOT
}

setupDatadog() {
    NODEIP=$1
    NODE_TYPE=$2
cat << 'EOF' > runDatadog.sh 
DD_AGENT_MAJOR_VERSION=7 DD_API_KEY=6ab706d12de9cb46db25a0aabab6a004 DD_SITE="datadoghq.com" bash -c "$(curl -L https://s3.amazonaws.com/dd-agent/scripts/install_script.sh)"
EOF

    if [[ "$NODE_TYPE" == "featurebase" ]]
    then
        echo "writing datadog config for featurebase node: $NODEIP, $NODE_TYPE, $BRANCH_NAME"
        writeDatadogConfigForFeaturebase
    else
        echo "writing datadog config for ingest node: $NODEIP, $NODE_TYPE, $BRANCH_NAME"
        writeDatadogConfigForIngest
    fi 

    echo "setting up datadog for $NODE_TYPE node: $NODEIP"
    echo "copying datadog config"
    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ./runDatadog.sh ./datadog.yaml ./conf.yaml ec2-user@${NODEIP}:.
    echo "installing datadog"
    cat ./runDatadog.sh
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "bash ./runDatadog.sh"
    echo "restarting datadog and updating config"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo systemctl stop datadog-agent"
    # update configuration and openmetrics for datadog 
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mv ./datadog.yaml /etc/datadog-agent/datadog.yaml"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo mv ./conf.yaml /etc/datadog-agent/conf.d/openmetrics.d/conf.yaml"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo systemctl start datadog-agent"
    echo "checking datadog status"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo systemctl status datadog-agent"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${NODEIP} "sudo yum install nc htop -y"
    rm runDatadog.sh datadog.yaml conf.yaml
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

# copy datagen to ingest nodes 
setupProducerNode(){
    IP=$1

    # download producer(datagen binary) from S3 
    DATAGEN_BINARY=datagen
    echo "Downloading Datagen binary"
    aws s3 cp s3://molecula-artifact-storage/idk/master/_latest/idk-linux-amd64/datagen $DATAGEN_BINARY --profile $AWS_PROFILE

    mountEBSVolume $IP
    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ${DATAGEN_BINARY} ec2-user@${IP}:
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${IP} "chmod ugo+x /home/ec2-user/${DATAGEN_BINARY}"
    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ./qa/scripts/delete/tremor_keys.yaml ./qa/scripts/delete/runProducer.sh ec2-user@${IP}:
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${IP} "sudo yum install jq htop tmux -y"
    setupKafkaServer ${IP}
    setupDatadog $IP "ingest"
}

setupConsumerNode(){
    IP=$1

    # download consumer(kafka-static binary) from S3
    KAFKA_STATIC=molecula-consumer-kafka-static
    echo "Downloading Molecular Consumer Kafka Static binary"
    aws s3 cp s3://molecula-artifact-storage/idk/master/_latest/idk-linux-amd64/molecula-consumer-kafka-static ./$KAFKA_STATIC --profile $AWS_PROFILE

    mountEBSVolume $IP
    
    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ./qa/scripts/delete/schema.json ./qa/scripts/delete/runConsumer.sh ec2-user@${IP}:
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${IP} "sudo yum install librdkafka -y"
    scp -i ~/.ssh/gitlab-featurebase-ci.pem -o "StrictHostKeyChecking no" ${KAFKA_STATIC} ec2-user@${IP}:
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${IP} "chmod ugo+x /home/ec2-user/${KAFKA_STATIC}"
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no ec2-user@${IP} "sudo yum install jq htop tmux nc -y"
    setupDatadog $IP "ingest"
}