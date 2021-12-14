#!/bin/bash

# To run script: ./deployNode.sh $PROFILE

# default to the VPC initially created
VPC=${VPC:-vpc-0582f594d7d2ca2d4}

INSTANCE_ID=""

function log() {
    printf "$@" >&2
}

function terminate() {
    if [ -n "$INSTANCE_ID" ]; then
        log "shutting down instance ID %s" "$INSTANCE_ID"
        doAws ec2 terminate-instances --instance-ids "$INSTANCE_ID"
    fi
}

# shut down instance on exit if we have created one
trap terminate 0

function doAws() {
        aws "$@" --profile "$PROFILE"
}

# SCP files to ec2-user@$IP
function doScp() {
    scp -o StrictHostKeyChecking=no -i ~/.ssh/gitlab-featurebase-ci.pem "$@" ec2-user@$IP:.
}

# Run command as ec2-user@$IP
function doSsh() {
    ssh -o StrictHostKeyChecking=no -i ~/.ssh/gitlab-featurebase-ci.pem ec2-user@$IP "$@"
}

# We need an amd64 Linux binary
function prep_binary() {
    GOOS=linux GOARCH=amd64 make build && mv featurebase featurebase_linux_amd64
}

function check_existing() {
    existing_states=$(doAws ec2 describe-instances --query 'Reservations[*].Instances[*].State.Name' --output text)
    log "instance states: %s" "$existing_states"
    case " $existing_states " in
    *" running "*)
        log "existing instance in running state, not restarting"
        return 1
        ;;
    esac
}

function get_config() {
    # get AMI, security group and subnet ID
    AMI=$(doAws ssm get-parameters --names "/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-ebs" --query 'Parameters[0].[Value]' --output text)
    if [[ $? > 0 ]]; then
        log "aws session manager failed to find AMI"
        return 1
    fi

    SECURITY_GROUP=$(doAws ec2 describe-security-groups --filters "Name=vpc-id,Values=$VPC" 'Name=group-name,Values=default' --query 'SecurityGroups[*].[GroupId]' --output text)
    if [[ $? > 0 || -z "$SECURITY_GROUP" ]]; then
        log "aws session manager failed to find security group"
        return 1
    fi

    SUBNET_ID=$(aws ec2 describe-subnets --filters 'Name=vpc-id,Values='"$VPC" 'Name=availability-zone,Values=us-east-2a' 'Name=tag:Name,Values=fbci-vpc-public-us-east-2a' --query 'Subnets[0].SubnetId' --output text --profile $PROFILE)
    if [[ $? > 0 || -z "$SUBNET_ID" ]]; then
        log "aws session manager failed to find subnet ID"
        return 1
    fi
}

function deploy_node() {
    # launch EC2 instance and get instance ID
    aws ec2 run-instances --image-id "$AMI" --instance-type "$INSTANCE" --security-group-ids "$SECURITY_GROUP" --subnet-id "$SUBNET_ID" --key-name gitlab-featurebase-ci --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=linux-amd64-node}]' --profile $PROFILE --user-data file://./qa/scripts/cloud-init.sh --iam-instance-profile Name=featurebase-ci-ssm > config.json
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
        IP=$(doAws ec2 describe-instances --instance-ids $INSTANCE_ID --filters 'Name=instance-state-name,Values=running' --query 'Reservations[*].Instances[*].PublicIpAddress' --output text)
        if [ -n "$IP" ]; then
            log "Public IP for EC2 instance: %s" "$IP"
            break
        fi

        if [[ $? > 0 ]]; then
            log "aws cli describe-instances command failed to find public IP"
            return 1
        fi

        sleep 5
    done

    sleep 60 # to allow enough time for node to be ready for use

    # copy featurebase binary and files to ec2 instance
    doScp featurebase_linux_amd64 ./qa/scripts/featurebase.conf ./qa/scripts/featurebase.service ./qa/scripts/setup.sh ./qa/scripts/regression.sh ./qa/scripts/perf.sh
    if [[ $? > 0 ]]; then
        log "scp of featurebase binary, service and config files to EC2 instance failed"
        return 1
    fi

    doSsh bash ./setup.sh || return 1
    doSsh bash ./regression.sh || return 1
    doSsh bash ./perf.sh || return 1
}

# Pass variables to shell script
PROFILE=$1
shift

# set some variables
INSTANCE="t3a.large"
REGION="us-east-2"

# check for existing copies; no point in running if one's already up
check_running || exit 1

# Prep featurebase binary
prep_binary || exit 1

# Obtain subnet info, etc.
get_config || exit 1

# get AMI, security group and subnet for EC2 instance,
# launch instance, save instance Id and run cloud-init to set up node env
deploy_node || exit 1

# Get IP for instance, scp featurebase binary, config and service files;
# set up featurebase config in node
initialize_featurebase || exit 1
