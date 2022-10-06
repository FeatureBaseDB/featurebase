#!/bin/sh

USERNAME=featurebase
GROUPNAME=featurebase
HOMEDIR=/var/lib/featurebase

getent group $GROUPNAME >/dev/null || groupadd -r $GROUPNAME
getent passwd $USERNAME >/dev/null || \
    useradd -r -g $GROUPNAME -d $HOMEDIR -s /sbin/nologin \
    -c "Featurebase Service Account" $USERNAME

exit 0
