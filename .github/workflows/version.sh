#!/bin/sh
current=$(git describe --tags | grep -Eo '[0-9]*?\.[0-9]*?\.[0-9]*')
array=($(echo $current | tr '.' '\n'))
array[2]=$((array[2]+1))
echo $(IFS="." ; echo "${array[*]}")