#!/bin/bash

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

# Execute RBF/Roaring benchmark.
RBF_PATH=gloat/data/1d/rbf/${DATE}.tar.gz
TXSRC=rbf gloat run -v -o $RBF_PATH "${BASH_SOURCE%/*}/etc/gloat/gh.1d.yml"

ROARING_PATH=gloat/data/1d/roaring/${DATE}.tar.gz
TXSRC=roaring gloat run -v -o $ROARING_PATH "${BASH_SOURCE%/*}/etc/gloat/gh.1d.yml"

# Generate graph from results.
gloat graph -layout 3,2 -size 2048,1024 -name utime,stime,heap_alloc,heap_inuse,heap_objects,num_gc -series rbf,roaring -o /tmp/output.png $RBF_PATH $ROARING_PATH

# Post graph to Slack with SHA.
curl -F file=@/tmp/output.png -F channels=C01HBFKRLGH -F "initial_comment=RBF vs Roaring, $DATE ($SHA)" -H "Authorization: Bearer $SLACK_OAUTH_TOKEN" https://slack.com/api/files.upload
