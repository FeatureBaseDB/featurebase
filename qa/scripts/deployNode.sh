#!/bin/bash

# To run script: ./deployNode.sh AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SSH_PRIVATE_KEY

function configure_env() {
    aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
    aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
    aws configure set region $REGION
    aws configure set aws_profile $PROFILE
    echo $AWS_SSH_PRIVATE_KEY > gitlab-featurebase-dev.pem
    chmod 400 gitlab-featurebase-dev.pem

    # set up ssh permissions 
    eval `ssh-agent -s`
    mkdir -p ~/.ssh
    echo $AWS_SSH_PRIVATE_KEY | ssh-add -
    chmod 700 /root/.ssh
}

function deploy_node() {
    # get AMI, security group and subnet ID 
    AMI=$(aws ssm get-parameters --names /aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-ebs --query 'Parameters[0].[Value]' --output text --profile $PROFILE)
    SECURITY_GROUP=$(aws ec2 describe-security-groups --filters Name=vpc-id,Values=vpc-03a4ba3d5b7c8f978 Name=group-name,Values=default --query 'SecurityGroups[*].[GroupId]' --output text --profile $PROFILE)
    SUBNET_ID=$(aws ec2 describe-subnets --filters 'Name=vpc-id,Values=vpc-03a4ba3d5b7c8f978' 'Name=availability-zone,Values=us-east-2a' --query 'Subnets[0].SubnetId' --output text --profile $PROFILE)

    # launch EC2 instance and get instance ID
    aws ec2 run-instances --image-id $AMI --instance-type $INSTANCE --security-group-ids $SECURITY_GROUP --subnet-id $SUBNET_ID --key-name gitlab-featurebase-dev --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=linux-amd64-node}]' --profile $PROFILE --user-data file://./qa/scripts/cloud-init.sh --iam-instance-profile Name=featurebase-dev-ssm > config.json
    INSTANCE_ID=$(jq '.Instances | .[] |.InstanceId' config.json | tr -d '"') 
}

function initialize_featurebase() {
    # get IP for node 
    for i in {0..24}
    do 
        IP=$(aws ec2 describe-instances --instance-ids $INSTANCE_ID --filters 'Name=instance-state-name, Values=running' --query 'Reservations[*].Instances[*].PublicIpAddress' --output text --profile $PROFILE)
        if [ -n "$IP" ]; then 
            break
        fi

        sleep 5
    done

    # copy featurebase binary and files to ec2 instance
    scp  -o StrictHostKeyChecking=no -i gitlab-featurebase-dev.pem featurebase_linux_amd64 ./qa/scripts/featurebase.conf ./qa/scripts/featurebase.service ec2-user@$IP:.

    # execute script to configure featurebase on the EC2 node 
    aws ssm send-command --document-name "AWS-RunShellScript" --instance-ids $INSTANCE_ID --cli-input-json file://./qa/scripts/configureFeatureBase.json --profile $PROFILE --region $REGION
}

function terminate_node() {
    aws ec2 terminate-instances --instance-ids $INSTANCE_ID
}

# Pass variables to shell script 
AWS_ACCESS_KEY_ID=$1
shift

AWS_SECRET_ACCESS_KEY=$1
shift

AWS_SSH_PRIVATE_KEY=$1
shift 

# set some variables 
PROFILE="default"
INSTANCE="t3a.large"
REGION="us-east-2"

# set up environment variables for aws, ssh
# and install needed packages 
configure_env
if [ $? > 0 ]; then 
    exit 1
fi

# get AMI, security group and subnet for EC2 instance,
# launch instance, save instance Id and run cloud-init to set up node env
deploy_node
if [ $? > 0 ]; then 
    exit 1
fi

# Get IP for instance, scp featurebase binary, config and service files;
# set up featurebase config in node 
initialize_featurebase
if [ $? > 0 ]; then 
    terminate_node
    exit 1
else 
    terminate_node
fi