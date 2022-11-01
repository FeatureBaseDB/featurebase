current=$(git describe --tags | grep -Eo '[0-9]*?\.[0-9]*?\.[0-9]*')
array=($(echo $current | tr '.' '\n'))
array[1]=$((array[1]+1))
echo $(IFS="." ; echo "${array[*]}")