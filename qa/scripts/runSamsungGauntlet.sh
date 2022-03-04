#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# requires TF_VAR_cluster_prefix env var to be set
if [ -z ${TF_VAR_cluster_prefix+x} ]; then 
    echo "setting TF_VAR_cluster_prefix";
    export TF_VAR_cluster_prefix="gauntlet-$(openssl rand -base64 12 | tr -d /=+ | cut -c -16)"
    echo "TF_VAR_cluster_prefix is set to '$TF_VAR_cluster_prefix'";  
else 
    echo "TF_VAR_cluster_prefix is set to '$TF_VAR_cluster_prefix'"; 
fi

$SCRIPT_DIR/setupSamsungGauntlet.sh
$SCRIPT_DIR/testSamsungGauntlet.sh
$SCRIPT_DIR/teardownSamsungGauntlet.sh
