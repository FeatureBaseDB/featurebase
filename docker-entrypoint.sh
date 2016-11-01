#!/bin/sh
# Docker entrypoint for Pilosa
# Implements runtime configuration via environment variables
sed -i -e s/HOSTNAMEVAR/${HOSTNAMEVAR}/ /etc/pilosa.conf
exec "$@"
