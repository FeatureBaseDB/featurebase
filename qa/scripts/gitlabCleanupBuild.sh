#!/bin/bash

CLEANUP_EC2_IDS=$(aws ec2 describe-instances --profile service-terraform --region us-east-2 --filters Name=instance-state-name,Values=pending,running "Name=tag:Name,Values=pipeline-${CI_PIPELINE_ID}-*" --query 'Reservations[*].Instances[*].[InstanceId]' --output text | tr '\n' ' ')
if [ -n "${CLEANUP_EC2_IDS}" ]
then
    echo "ec2 Instances found, terminating."
    echo $CLEANUP_EC2_IDS
    IFS=', ' read -r -a array <<< "$CLEANUP_EC2_IDS"
    aws ec2 terminate-instances --profile service-terraform --region us-east-2 --instance-ids "${array[@]}"
    
    res=$?
    if (( $res != 0 )); then 
        echo "Error: instances not terminated - ${CLEANUP_EC2_IDS}"
        exit $res
    fi
else
    echo 'No hanging ec2 instances'
fi

CLEANUP_CF_ID=$(aws cloudformation list-stacks --region us-east-2 --stack-status-filter CREATE_IN_PROGRESS CREATE_COMPLETE --query  "StackSummaries[?contains(StackName, \`pipeline-${CI_PIPELINE_ID}\`) == \`true\`].[StackName]" --output text  | tr '\n' ' ')
if [ -n "${CLEANUP_CF_ID}" ]
then
    echo "CF stacks found, deleting."
    echo $CLEANUP_CF_ID
    IFS=', ' read -r -a array <<< "$CLEANUP_CF_ID"
    for element in "${array[@]}"
    do
        aws cloudformation delete-stack --region us-east-2 --stack-name $element --retain-resources DeploymentEC2Role
        res=$?
        if (( $res != 0 )); then 
            echo "Error: stacks not deleted - ${CLEANUP_CF_ID}"
            exit $res
        fi
    done
else
    echo "No CF stacks to cleanup"
fi