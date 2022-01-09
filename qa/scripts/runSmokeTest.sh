#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# requires TF_VAR_gitlab_token env var to be set
if [ -z ${TF_VAR_gitlab_token+x} ]; then echo "TF_VAR_gitlab_token is unset"; else echo "TF_VAR_gitlab_token is set to '$TF_VAR_gitlab_token'"; fi

# requires TF_VAR_branch env var to be set
if [ -z ${TF_VAR_branch+x} ]; then echo "TF_VAR_branch is unset"; else echo "TF_VAR_branch is set to '$TF_VAR_branch'"; fi

# requires TF_VAR_cluster_prefix env var to be set
if [ -z ${TF_VAR_cluster_prefix+x} ]; then 
    echo "setting TF_VAR_cluster_prefix";
    export TF_VAR_cluster_prefix="smoke-$(openssl rand -base64 12 | tr -d /=+ | cut -c -16)"
    echo "TF_VAR_cluster_prefix is set to '$TF_VAR_cluster_prefix'";  
else 
    echo "TF_VAR_cluster_prefix is set to '$TF_VAR_cluster_prefix'"; 
fi

$SCRIPT_DIR/setupSmokeTest.sh
$SCRIPT_DIR/testSmokeTest.sh
$SCRIPT_DIR/teardownSmokeTest.sh
