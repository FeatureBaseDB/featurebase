#!/bin/bash
# Docker entrypoint for Pilosa
# Implements runtime configuration via environment variables
CONF_FILE=/etc/pilosa.conf
cat << EOF > $CONF_FILE
data-dir = "$DATA_DIR"
host = "$HOSTNAMEVAR:$PORT"

[cluster]
replicas = $REPLICAS
EOF
for ((i = 0; i < $NODES; i++)); do
    printf "\n[[cluster.node]]\nhost = \"pil${i}:15000\"\n" >> $CONF_FILE
done
exec "$@"
