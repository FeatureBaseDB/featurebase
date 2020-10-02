#!/bin/bash

set +x
export PATH=.:${PATH}

# unpack the sample Molecula Pilosa cluster.
tar xf backups.tar.gz


# check if repair is needed.
pilosa-fsck -replicas 3 backups/node0/pilosa backups/node1/pilosa backups/node2/pilosa backups/node3/pilosa


# yes, so do the repairs. This can be done first (only) as well.
#
pilosa-fsck -fix -replicas 3 backups/node0/pilosa backups/node1/pilosa backups/node2/pilosa backups/node3/pilosa


# check again if you like
#
pilosa-fsck -replicas 3 backups/node0/pilosa backups/node1/pilosa backups/node2/pilosa backups/node3/pilosa
