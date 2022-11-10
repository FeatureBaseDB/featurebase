#!/bin/sh

USERNAME=featurebase
GROUPNAME=featurebase
HOMEDIR=/var/lib/featurebase

cleanInstall() {
    printf "\033[32m Adding featurebase user\033[0m\n"

    getent group $GROUPNAME >/dev/null || groupadd -r $GROUPNAME
    getent passwd $USERNAME >/dev/null || \
        useradd -r -g $GROUPNAME -d $HOMEDIR -s /sbin/nologin \
        -c "Featurebase Service Account" $USERNAME
}

upgrade() {
    printf "\033[32m Stopping featurebase service before upgrade\033[0m\n"
    systemctl stop featurebase ||:

    printf "\033[32m Disabling featurebase service before upgrade\033[0m\n"
    systemctl disable featurebase ||:
}

# Step 2, check if this is a clean install or an upgrade
action="$1"
if  [ "$1" = "configure" ] && [ -z "$2" ]; then
  # Alpine linux does not pass args, and deb passes $1=configure
  action="install"
elif [ "$1" = "configure" ] && [ -n "$2" ]; then
    # deb passes $1=configure $2=<current version>
    action="upgrade"
fi

case "$action" in
  "1" | "install")
    cleanInstall
    ;;
  "2" | "upgrade")
    upgrade
    ;;
  *)
    # $1 == version being installed
    printf "\033[32m Alpine\033[0m"
    cleanInstall
    ;;
esac
