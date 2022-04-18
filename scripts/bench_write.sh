#!/bin/bash
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
make -C $PILOSA_SRC install

# Retrieve current SHA.
SHA=$(git -C $PILOSA_SRC rev-parse HEAD)

# Format current date.
DATE=$(date '+%Y%m%d')

for FILENAME in gh.1m.yml gh.issues.keyed.yml gh.issues.unkeyed.yml gh.issues.autogenerate.yml
do
	WORKFLOW_PATH="${BASH_SOURCE%/*}/etc/gloat/${FILENAME}"
	WORKFLOW_NAME="$(gloat workflow name $WORKFLOW_PATH)"
	TITLE="RBF vs Roaring, $WORKFLOW_NAME, $DATE ($SHA)"

	# Execute RBF/Roaring benchmark.
	RBF_PATH=gloat/data/1m/rbf/${DATE}.tar.gz
	STORAGE_BACKEND=rbf gloat run -v -o $RBF_PATH $WORKFLOW_PATH

	ROARING_PATH=gloat/data/1m/roaring/${DATE}.tar.gz
	STORAGE_BACKEND=roaring gloat run -v -o $ROARING_PATH $WORKFLOW_PATH

	# Generate graph from results.
	gloat graph -layout 2,5 -size 5120,820 -title "$TITLE" -name utime,stime,heap_alloc,heap_inuse,heap_objects,num_gc,rchar,wchar,syscr,syscw -series rbf,roaring -o /tmp/output.png $RBF_PATH $ROARING_PATH

	# Post graph to Slack with SHA.
	curl -F file=@/tmp/output.png -F channels=C01HBFKRLGH -F "initial_comment=$TITLE" -H "Authorization: Bearer $SLACK_OAUTH_TOKEN" https://slack.com/api/files.upload
done
