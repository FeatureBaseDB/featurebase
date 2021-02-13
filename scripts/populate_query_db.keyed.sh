#!/bin/bash
set -e

# This script generates data query load testing to be run against.
#
# Environment variables:
#   - STORAGE_BACKEND: Transaction store type ("roaring", "rbf")
#   - CACHEDIR: Path to local GitHub Archive data, if available.

# Require environment variables.
: "${STORAGE_BACKEND:?Must set STORAGE_BACKEND environment variable}"
: "${GHCACHEDIR:''}"

echo "Starting pilosa"
pilosa server --data-dir ~/pilosa.query.keyed.${STORAGE_BACKEND} --storage.backend ${STORAGE_BACKEND} & pid_pilosa=$!
sleep 5

echo ""
echo "Importing GitHub Archive"
molecula-consumer-github -i issues -r url --record-type issue --batch-size=100000 \
	--start-time 2020-01-01T00:00:00Z --end-time 2020-01-31T23:00:00Z \
	--cache-dir "$GHCACHEDIR"

echo ""
echo "Import complete, shutting down pilosa"

sleep 5
kill $pid_pilosa
