#!/bin/bash
tests_failed=0

# test validity of featurebase.service
systemd-analyze verify /etc/systemd/system/featurebase.service 
exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo 'featurebase.redhat.service invalid'
    tests_failed=1
fi

# test validity of featurebase.conf
mkdir /var/log/molecula
featurebase_bin='/usr/local/bin/featurebase'
config_file='/etc/featurebase.conf'
if ! $featurebase_bin -c $config_file  holder /dev/null 2>&1; then
    echo 'featurebase.conf is invalid'
    tests_failed=1
fi

# print success if both passed
if [ $tests_failed -eq 0 ]; then
    echo 'featurebase.redhat.service is valid'
    echo 'featurebase.conf is valid'
fi

# tests_failed set to 0 if none of the tests failed
# otherwise set to non-zero
exit $tests_failed
