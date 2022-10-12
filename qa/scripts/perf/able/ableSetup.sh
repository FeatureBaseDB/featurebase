#!/bin/bash

# To run script: ./ableSetup.sh <BRANCH_NAME>
export TF_IN_AUTOMATION=1

BRANCH_NAME=$1
shift

if [ -z ${TF_VAR_cluster_prefix+x} ]; then
    echo "TF_VAR_cluster_prefix is unset";
    exit 1
else
    echo "TF_VAR_cluster_prefix is set to '$TF_VAR_cluster_prefix'";
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source $SCRIPT_DIR/../../utilCluster.sh

pushd ./qa/tf/perf/able
echo "Running terraform init..."
terraform init -input=false
echo "Running terraform apply..."

okay=false
# This logic is gratuitously complicated because I want visibility
# into how it's working, or not-working. The chances are this should
# just be a test against the exit status of terraform apply.
tries=1
while ! $okay && [ $tries -le $max_tries ] ; do
	echo "Try $tries/$max_tries, running terraform..."
	terraform apply -input=false -auto-approve
	tf=$?
	terraform output -json > outputs.json
	echo "Outputs:"
	cat outputs.json
	must_get_value outputs.json TMP_I .ingest_ips 0 '"value"' 0
	must_get_value outputs.json TMP_D .data_node_ips 0 '"value"' 0
	echo "TF status: $tf, ingest_ips $TMP_I, data_node_ips $TMP_D"
	case $TMP_I.$TMP_D in
	*null*)	echo >&2 "looks like we failed, null in IPs."
		tries=$(expr $tries + 1)
		;;
	*)	okay=true
		;;
	esac
done
echo "okay $okay, tries $tries"
if ! $okay; then
	echo >&2 "didn't start terraform successfully, giving up"
	exit 1
fi
terraform output -json > outputs.json
popd

# path for ebs device
EBS_DEVICE_NAME=/dev/nvme1n1

# featurebase architecture
FB_BINARY=featurebase_linux_arm64

# get the first ingest host
must_get_value ./qa/tf/perf/able/outputs.json INGESTNODE0 .ingest_ips 0 '"value"' 0
echo "using INGESTNODE0 ${INGESTNODE0}"

# get the first data host
must_get_value ./qa/tf/perf/able/outputs.json DATANODE0 .data_node_ips 0 '"value"' 0
echo "using DATANODE0 ${DATANODE0}"

case ${INGESTNODE0}${DATANODE0} in
*null*)	echo >&2 "didn't get nodes, giving up early"
	exit 1
	;;
esac

must_get_value ./qa/tf/perf/able/outputs.json DEPLOYED_CLUSTER_PREFIX .cluster_prefix 0 '"value"'
echo "Using DEPLOYED_CLUSTER_PREFIX: ${DEPLOYED_CLUSTER_PREFIX}"

must_get_value ./qa/tf/perf/able/outputs.json DEPLOYED_CLUSTER_REPLICA_COUNT .fb_cluster_replica_count 0 '"value"'
echo "Using DEPLOYED_CLUSTER_REPLICA_COUNT: ${DEPLOYED_CLUSTDEPLOYED_CLUSTER_REPLICA_COUNTER_PREFIX}"

must_get_value ./qa/tf/perf/able/outputs.json DEPLOYED_DATA_IPS .data_node_ips 0 '"value"' ""
echo "DEPLOYED_DATA_IPS: {"
echo "${DEPLOYED_DATA_IPS}"
echo "}"

DEPLOYED_DATA_IPS_LEN=`echo "$DEPLOYED_DATA_IPS" | wc -l`

must_get_value ./qa/tf/perf/able/outputs.json DEPLOYED_INGEST_IPS .ingest_ips 0 '"value"' ""
echo "DEPLOYED_INGEST_IPS: {"
echo "${DEPLOYED_INGEST_IPS}"
echo "}"

DEPLOYED_INGEST_IPS_LEN=`echo "$DEPLOYED_INGEST_IPS" | wc -l`

#wait until we can connect to one of the hosts
for i in {0..24}
do
    ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no -o ConnectTimeout=10 ec2-user@${DATANODE0} "pwd"
    if [ $? -eq 0 ]
    then
        echo "Cluster is up after ${i} tries."
        break
    fi
    sleep 10
done

ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem -o StrictHostKeyChecking=no -o ConnectTimeout=10 ec2-user@${DATANODE0} "pwd"
if [ $? -ne 0 ]
then
    echo "Unable to connect to cluster - giving up"
    exit 1
fi

setupClusterNodes

# verify featurebase running
echo "Verifying featurebase cluster running..."
curl -s http://${DATANODE0}:10101/status
if (( $? != 0 ))
then
    echo "Featurebase cluster not running"
    exit 1
fi

echo "Cluster running."



