#!/bin/bash

# To run script: ./deployNode.sh $PROFILE

# default to the VPC initially created
VPC=${VPC:-vpc-0582f594d7d2ca2d4}

function deploy_node() {
    # get AMI, security group and subnet ID 
    AMI=$(aws ssm get-parameters --names /aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-ebs --query 'Parameters[0].[Value]' --output text --profile $PROFILE)
    if [[ $? > 0 ]]; then 
        echo "aws session manager failed to find AMI"
        exit 1
    fi

    SECURITY_GROUP=$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$VPC" Name=group-name,Values=default --query 'SecurityGroups[*].[GroupId]' --output text --profile $PROFILE)
    if [[ $? > 0 ]]; then 
        echo "aws session manager failed to find security group"
        exit 1
    fi
    
    SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC" 'Name=availability-zone,Values=us-east-2a' --query 'Subnets[0].SubnetId' --output text --profile $PROFILE)
    if [[ $? > 0 ]]; then 
        echo "aws session manager failed to find subnet ID"
        exit 1
    fi

    # launch EC2 instance and get instance ID
    aws ec2 run-instances --image-id $AMI --instance-type $INSTANCE --security-group-ids $SECURITY_GROUP --subnet-id $SUBNET_ID --key-name gitlab-featurebase-dev --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=linux-amd64-node}]' --profile $PROFILE --user-data file://./qa/scripts/cloud-init.sh --iam-instance-profile Name=featurebase-dev-ssm > config.json
    if [[ $? > 0 ]]; then 
        echo "aws run-instances failed to launch a new EC2 instance"
        exit 1
    fi

    INSTANCE_ID=$(jq '.Instances | .[0] |.InstanceId' config.json | tr -d '"') 
    echo "aws run-instances succeeded in launching a new EC2 instance with instance ID: " $INSTANCE_ID
}

function initialize_featurebase() {
    # get IP for node 
    for i in {0..24}
    do 
        IP=$(aws ec2 describe-instances --instance-ids $INSTANCE_ID --filters 'Name=instance-state-name, Values=running' --query 'Reservations[*].Instances[*].PublicIpAddress' --output text --profile $PROFILE)
        if [ -n "$IP" ]; then
            echo "Public IP for EC2 instance found: " $IP 
            break
        fi

        if [[ $? > 0 ]]; then 
            echo "aws cli describe-instances command failed to find public IP"
            terminate_node
            exit 1
        fi

        sleep 5
    done
    
    sleep 60 # to allow enough time for node to be ready for use

    # copy featurebase binary and files to ec2 instance
    scp  -o StrictHostKeyChecking=no -i gitlab-featurebase-dev.pem featurebase_linux_amd64 ./qa/scripts/featurebase.conf ./qa/scripts/featurebase.service ec2-user@$IP:.
    if [[ $? > 0 ]]; then 
        echo "scp of featurebase binary, service and config files to EC2 instance failed"
        terminate_node
        exit 1
    fi

    # execute script to configure featurebase on the EC2 node 
    aws ssm send-command --document-name "AWS-RunShellScript" --instance-ids $INSTANCE_ID --cli-input-json file://./qa/scripts/configureFeatureBase.json --profile $PROFILE --region $REGION
    if [[ $? > 0 ]]; then 
        echo "aws cli session manager send-command failed"
        terminate_node
        exit 1
    fi
}

function terminate_node() {
    aws ec2 terminate-instances --instance-ids $INSTANCE_ID --profile $PROFILE 
}

# Pass variables to shell script
PROFILE=$1
shift 

# set some variables 
INSTANCE="t3a.large"
REGION="us-east-2"

# get AMI, security group and subnet for EC2 instance,
# launch instance, save instance Id and run cloud-init to set up node env
deploy_node

# Get IP for instance, scp featurebase binary, config and service files;
# set up featurebase config in node 
initialize_featurebase

# terminate node 
terminate_node
