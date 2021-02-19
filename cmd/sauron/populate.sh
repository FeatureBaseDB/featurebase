#!/bin/bash
mod=20
for i in `seq 100 1`;do
b=$(dc -e "$i $mod %p")
[[ $b == 0 ]] && mod=$(($mod+1)) 
r=$(($b+30))
payload="Set($i, j=$r)"
echo $payload
curl -XPOST "localhost:10101/index/i/query" -d "$payload"
done
