#!/bin/bash


#openssl rand -base64 32 | tr -d /=+ | cut -c -16

./setupSamsungGauntlet.sh
./testSamsungGauntlet.sh
./teardownSamsungGauntlet.sh
