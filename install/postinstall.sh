#!/bin/sh

cleanInstall() {
    printf "\033[32m Reload the service unit from disk\033[0m\n"
    systemctl daemon-reload ||:
    printf "\033[32m Enable featurebase service\033[0m\n"
    systemctl enable featurebase ||:
}

upgrade() {
    printf "\033[32m Reload the service unit from disk\033[0m\n"
    systemctl daemon-reload ||:

    printf "\033[32m Enabling featurebase after upgrade\033[0m\n"
    systemctl enable featurebase ||:

    printf "\033[32m Starting featurebase after upgrade\033[0m\n"
    systemctl start featurebase ||:
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
