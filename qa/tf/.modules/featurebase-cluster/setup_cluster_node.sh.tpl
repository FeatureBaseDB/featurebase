#!/bin/bash

#path to the featurebase.conf file
CONFIG_FILE_PATH="/etc/featurebase.conf"
#path to the featurebase.service file
SERVICE_FILE_PATH="/etc/systemd/system/featurebase.service"

AWS_INSTANCE_ID=""
#IP of this node
PRIVATE_IP=""
PRIVATE_IP_INDEX=-1
#IPs of the cluster
CLUSTER_IPS=""

get_aws_instance_id() {
    echo "Getting AWS instance ID..."
    while true
    do
        curl -s http://169.254.169.254/latest/meta-data/instance-id > /dev/null
        if [ $? -eq 0 ]
        then
            break
        fi
    done
    AWS_INSTANCE_ID=`curl http://169.254.169.254/latest/meta-data/instance-id`
    echo "AWS instance ID is: $${AWS_INSTANCE_ID}"
}

wait_on_all_cluster_ips() {
    echo "Waiting on all cluster IPs..."
    # get IP for node 
    IPS=$(aws ec2 describe-instances --filters "Name=instance-state-name, Values=running" "Name=tag:Role, Values=cluster_node" "Name=tag:Prefix, Values=${cluster_prefix}" --query 'Reservations[*].Instances[*].PrivateIpAddress' --output text --region ${region})
    IP_LENGTH=`echo "$IPS" | wc -l`

    for i in {0..24}
    do 
        echo "Comparing $${IP_LENGTH} with ${node_count}"
        if [ $IP_LENGTH == "${node_count}" ]; then
            echo "Cluster is up after $${i} tries." 
            break
        fi
        sleep 10s
    done

    if [ $IP_LENGTH != "${node_count}" ]; then
        echo "Timed out waiting for cluster to be available $${IP_LENGTH} actual nodes compared with ${node_count} desire nodes."
        exit 1
    fi
}

get_private_ip() {
    echo "Getting private IP address..."
    PRIVATE_IP=$(aws ec2 describe-instances --filters "Name=instance-state-name, Values=running" "Name=instance-id,Values=$${AWS_INSTANCE_ID}" --query 'Reservations[*].Instances[*].PrivateIpAddress' --output text --region ${region})
    echo "Private IP is $${PRIVATE_IP}"
}

get_cluster_ips() {
    echo "Getting cluster IPs..."
    # get IP for node 
    IPS=$(aws ec2 describe-instances --filters "Name=instance-state-name, Values=running" "Name=tag:Role, Values=cluster_node" "Name=tag:Prefix, Values=${cluster_prefix}" --query 'Reservations[*].Instances[*].PrivateIpAddress' --output text --region ${region})
    IP_LENGTH=`echo "$IPS" | wc -l`

    IFS=$'\n'
    cnt=0
    for ip in $IPS
    do
        echo $cnt $ip
        if (($cnt + 1 != $IP_LENGTH)) 
        then
            CLUSTER_IPS="$${CLUSTER_IPS}p$${cnt}=http://$ip:10301,"
        else
            CLUSTER_IPS="$${CLUSTER_IPS}p$${cnt}=http://$ip:10301"
        fi
        echo "comparing $ip to $PRIVATE_IP"
        if [ "$ip" = "$PRIVATE_IP" ]; then
            PRIVATE_IP_INDEX=$cnt
        fi
        cnt=$((cnt+1))
    done

    echo "CLUSTER_IPS are: $${CLUSTER_IPS}"
}

write_featurebase_config_file() {
    echo "Writing featurebase.conf file..."
    cat << EOT > $${CONFIG_FILE_PATH}
name = "p$${PRIVATE_IP_INDEX}"
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

    name = "${cluster_prefix}"
    replicas = ${fb_cluster_replica_count}

[etcd]

    listen-client-address = "http://$${PRIVATE_IP}:10401"
    listen-peer-address = "http://$${PRIVATE_IP}:10301"
    initial-cluster = "$${CLUSTER_IPS}"

[metric]

    service = "prometheus"
EOT

    echo "featurebase.conf written to $${CONFIG_FILE_PATH}."
}

write_featurebase_service_file() {
    echo "Writing featurebase.service file..."
    cat << EOT > $${SERVICE_FILE_PATH}
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

    echo "featurebase.service written to $${SERVICE_FILE_PATH}."
    
}

#get the instance id
get_aws_instance_id

#copy the script so we can look at it later if needed
sudo cp /var/lib/cloud/instances/$${AWS_INSTANCE_ID}/user-data.txt /home/ec2-user/setup_cluster_node.sh

#wait for the count of nodes to equal requested nodes
wait_on_all_cluster_ips

#get private ip
get_private_ip

#generate cluster ips
get_cluster_ips

#write the featurebase config file
write_featurebase_config_file

#write the featurebase service file
write_featurebase_service_file

#get the featurebase binary and put in in the right spot
echo "Getting featurebase binary..."
curl --fail --header "PRIVATE-TOKEN: ${gitlab_token}" -o "/home/ec2-user/featurebase_linux_arm64" https://gitlab.com/api/v4/projects/molecula%2Ffeaturebase/jobs/artifacts/${branch}/raw/featurebase_linux_arm64?job=build%20for%20linux%20arm64
chown ec2-user:ec2-user "/home/ec2-user/featurebase_linux_arm64"
chmod ugo+x "/home/ec2-user/featurebase_linux_arm64"

mv /home/ec2-user/featurebase_linux_arm64 /usr/local/bin/featurebase
echo "featurebase binary copied."

sudo mkdir /data
sudo mkfs.ext4 /dev/nvme1n1
sudo mount /dev/nvme1n1 /data

adduser molecula
sudo mkdir /var/log/molecula
sudo chown molecula /var/log/molecula
sudo mkdir -p /data/featurebase
sudo chown molecula /data/featurebase
sudo systemctl daemon-reload
sudo systemctl start featurebase
sudo systemctl enable featurebase
sudo systemctl status featurebase

echo "Done!"