#!/bin/bash -x
set -e

# This script runs benchmarks and posts them to Slack's #nightly channel.
# The caller of the script should `git pull` on the pilosa repo before executing.
#
# Environment variables:
#   - PILOSA_SRC: Path to pilosa src directory.
#   - SLACK_OAUTH_TOKEN: Token used to post to Slack.

# Require environment variables.
: "${PILOSA_SRC:?Must set PILOSA_SRC environment variable}"
: "${SLACK_OAUTH_TOKEN:?Must set SLACK_OAUTH_TOKEN environment variable}"

# Build pilosa into GOBIN.
make -C $PILOSA_SRC install install-bench

# Retrieve current SHA.
SHA=$(git -C $PILOSA_SRC rev-parse HEAD)

# Format current date.
DATE=$(date '+%Y%m%d')

for TYPE in row row-bsi row-range count count-keyed intersect union difference xor groupby topk
do
	WORKFLOW_PATH="${BASH_SOURCE%/*}/etc/gloat/query.${TYPE}.yml"
	WORKFLOW_NAME="$(gloat workflow name $WORKFLOW_PATH)"
	
	# Execute RBF/Roaring benchmark.
	STARTTIME=$(date +%s)
	RBF_PATH=gloat/data/query/${TYPE}/rbf/${DATE}.tar.gz
	STORAGE_BACKEND=rbf gloat run -v -o "$RBF_PATH" $WORKFLOW_PATH
	RBF_ELAPSED=$(($(date +%s) - $STARTTIME))
	RBF_LATENCY=$(gloat metric -n -name request_avg_latency "$RBF_PATH")

	STARTTIME=$(date +%s)
	ROARING_PATH=gloat/data/query/${TYPE}/roaring/${DATE}.tar.gz
	STORAGE_BACKEND=roaring gloat run -v -o "$ROARING_PATH" $WORKFLOW_PATH
	ROARING_ELAPSED=$(($(date +%s) - $STARTTIME))
	ROARING_LATENCY=$(gloat metric -n -name request_avg_latency "$ROARING_PATH")

	TITLE="$WORKFLOW_NAME, $DATE ($SHA) elapsed rbf=$RBF_ELAPSEDroaring=$ROARING_ELAPSED> latency rbf=$RBF_LATENCY roaring=$ROARING_LATENCY"

	# Generate graph from results.
	gloat graph -layout 2,5 -size 5120,820 -title "$TITLE" -name utime,stime,heap_alloc,heap_inuse,heap_objects,num_gc,rchar,wchar,syscr,syscw -series rbf,roaring -o /tmp/output.png $RBF_PATH $ROARING_PATH

	# Post graph to Slack with SHA.
	curl -F file=@/tmp/output.png -F channels=C01HBFKRLGH -F "initial_comment=$TITLE" -H "Authorization: Bearer $SLACK_OAUTH_TOKEN" https://slack.com/api/files.upload
done
