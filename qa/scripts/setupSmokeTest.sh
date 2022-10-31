#!/bin/bash
set -x
# how often should we try to do our terraform setup?
max_tries=3

# To run script: ./setupSmokeTest.sh <BRANCH_NAME>
ADMIN_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiYWRtaW4ifQ.I1iCgk1VU7m6e-En4ACTHIs6V2dZpy_8j2blSSo7K3U
READER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoicmVhZGVyIn0.QcHy_W6oAYFgdBWy1CqLr55HcOyymn5zAXPJUKCvQE4
WRITER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoid3JpdGVyIn0.aEk-12xP9RJeXog4MHO8LhuQFEjNNG2BcWDcMzSX_HI
export TF_IN_AUTOMATION=1
export AUTH_ENABLED=1

BRANCH_NAME=$1

echo "Running tests for branch ${BRANCH_NAME}"

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
. $SCRIPT_DIR/utilCluster.sh

pushd ./qa/tf/ci/smoketest
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
	get_value outputs.json TMP_I .ingest_ips 0 '"value"' 0
	get_value outputs.json TMP_D .data_node_ips 0 '"value"' 0
	echo "TF status: $tf, ingest_ips $TMP_I, data_node_ips $TMP_D"
	okay_i=false
	okay_d=false
	case $TMP_I in
	null)	echo >&2 "looks like we failed, null in ingest IPs."
		;;
	"")	echo >&2 "looks like we failed, empty string in ingest IPs."
		;;
	*)	okay_i=true
		;;
	esac
	case $TMP_D in
	null)	echo >&2 "looks like we failed, null in data IPs."
		;;
	"")	echo >&2 "looks like we failed, empty string in data IPs."
		;;
	*)	okay_d=true
		;;
	esac
	if $okay_i && $okay_d; then
		okay=true
	else
		echo >&2 "retrying"
		tries=$(expr $tries + 1)
	fi
done
echo "okay $okay, tries $tries"
if ! $okay; then
	echo >&2 "didn't start terraform successfully, giving up"
	exit 1
fi
terraform output -json > outputs.json
echo "Outputs:"
cat outputs.json
popd

# path for ebs device
EBS_DEVICE_NAME=/dev/nvme1n1

# featurebase architecture
FB_BINARY=featurebase_linux_arm64

# get the first ingest host
must_get_value ./qa/tf/ci/smoketest/outputs.json INGESTNODE0 .ingest_ips 0 '"value"' 0
echo "using INGESTNODE0 ${INGESTNODE0}"

# get the first data host
must_get_value ./qa/tf/ci/smoketest/outputs.json DATANODE0 .data_node_ips 0 '"value"' 0
echo "using DATANODE0 ${DATANODE0}"

must_get_value ./qa/tf/ci/smoketest/outputs.json DEPLOYED_CLUSTER_PREFIX .cluster_prefix 0 '"value"'
echo "Using DEPLOYED_CLUSTER_PREFIX: ${DEPLOYED_CLUSTER_PREFIX}"

must_get_value ./qa/tf/ci/smoketest/outputs.json DEPLOYED_CLUSTER_REPLICA_COUNT .fb_cluster_replica_count 0 '"value"'
echo "Using DEPLOYED_CLUSTER_REPLICA_COUNT: ${DEPLOYED_CLUSTDEPLOYED_CLUSTER_REPLICA_COUNTER_PREFIX}"

must_get_value ./qa/tf/ci/smoketest/outputs.json DEPLOYED_DATA_IPS .data_node_ips 0 '"value"' ""
echo "DEPLOYED_DATA_IPS: {"
echo "${DEPLOYED_DATA_IPS}"
echo "}"

DEPLOYED_DATA_IPS_LEN=`echo "$DEPLOYED_DATA_IPS" | wc -l`

must_get_value ./qa/tf/ci/smoketest/outputs.json DEPLOYED_INGEST_IPS .ingest_ips 0 '"value"' ""
echo "DEPLOYED_INGEST_IPS: {"
echo "${DEPLOYED_INGEST_IPS}"
echo "}"

DEPLOYED_INGEST_IPS_LEN=`echo "$DEPLOYED_INGEST_IPS" | wc -l`

echo "Writing config.py file..."
cat << EOT > config.py
datanode0="${DATANODE0}"
admin_token="${ADMIN_TOKEN}"
reader_token="${READER_TOKEN}"
writer_token="${WRITER_TOKEN}"
EOT
cp config.py ./qa/testcases/smoketest/config.py
echo "Writing config.sh file..."
cat << EOT > config.sh
export DATANODE0="${DATANODE0}"
export ADMIN_TOKEN="${ADMIN_TOKEN}"
export READER_TOKEN="${READER_TOKEN}"
export WRITER_TOKEN="${WRITER_TOKEN}"
authcurl() {
    curl -H "Authorization: Bearer ${ADMIN_TOKEN}" -k "\$@"
}
EOT
cp config.sh ./qa/testcases/bug-repros/config.sh

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

setupClusterNodes $BRANCH_NAME

# verify featurebase running
for i in {0..24}; do
    sleep 1
    curl -k -v https://${DATANODE0}:10101/status -H "Authorization: Bearer ${ADMIN_TOKEN}"
    if [ $? -eq 0 ]; then
        echo "Cluster is up after ${i} tries"
        exit 0
    fi
done
echo >&2 "Featurebase cluster not running."
exit 1


