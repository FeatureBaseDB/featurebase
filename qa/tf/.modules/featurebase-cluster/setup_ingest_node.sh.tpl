#!/bin/bash

AWS_INSTANCE_ID=""

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

wait_on_all_ingest_ips() {
    echo "Waiting on all cluster IPs..."
    # get IP for node 
    IPS=$(aws ec2 describe-instances --filters "Name=instance-state-name, Values=running" "Name=tag:Role, Values=ingest_node" "Name=tag:Prefix, Values=${cluster_prefix}" --query 'Reservations[*].Instances[*].PrivateIpAddress' --output text --region ${region})
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

#copy the script so we can look at it later if needed
sudo cp /var/lib/cloud/instances/$${AWS_INSTANCE_ID}/user-data.txt ~/setup_ingest_node.sh

#get the instance id
get_aws_instance_id

#wait for the count of nodes to equal requested nodes
wait_on_all_ingest_ips

echo "Getting featurebase binary..."
curl --fail --header "PRIVATE-TOKEN: ${gitlab_token}" -o "/home/ec2-user/featurebase_linux_arm64" https://gitlab.com/api/v4/projects/molecula%2Ffeaturebase/jobs/artifacts/${branch}/raw/featurebase_linux_arm64?job=build%20for%20linux%20arm64
chown ec2-user:ec2-user "/home/ec2-user/featurebase_linux_arm64"
chmod ugo+x "/home/ec2-user/featurebase_linux_arm64"

mv /home/ec2-user/featurebase_linux_arm64 /usr/local/bin/featurebase
echo "featurebase binary copied."


sudo mkdir /data
sudo mkfs.ext4 /dev/nvme1n1
sudo mount /dev/nvme1n1 /data

sudo chown -R ec2-user /data

echo "Installing pytest"
pip3 install -U pytest
pip3 install -U requests

echo "Done."



