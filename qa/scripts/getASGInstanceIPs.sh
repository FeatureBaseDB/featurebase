#!/bin/bash

# usage of this script:
# ./getInstanceIPs.sh my-asg-name my-aws-profile-name

# if you have used the FeatureBaseCLusterCFTTremor.yaml cloudoformation template, 
# then you can do the following to save the IPs of the auto scaling groups:

# for the featurebase data nodes ips:
# DEPLOYED_DATA_IPS=$(./getASGInstanceIPs.sh ${STACK_NAME}-asg ${AWS_PROFILE})

# for the producer nodes:
# PRODUCER_IPS=$(./getASGInstanceIPs.sh ${STACK_NAME}-producer-asg ${AWS_PROFILE})

# for the consumer nodes:
# CONSUMER_IPS=$(./getASGInstanceIPs.sh ${STACK_NAME}-consumer-asg ${AWS_PROFILE})

for i in `aws autoscaling describe-auto-scaling-groups --auto-scaling-group-name $1 --profile $2 --output=json | grep -i instanceid  | awk '{ print $2}' | cut -d',' -f1| sed -e 's/"//g'`
do
    aws ec2 describe-instances --output=json --instance-ids $i --profile $2 | grep -i PrivateIpAddress | awk '{ print $2 }' | head -1 | cut -d"," -f1| sed -e 's/"//g'
done;