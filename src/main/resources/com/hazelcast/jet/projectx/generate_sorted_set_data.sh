#!/usr/bin/env bash
if [[ "$#" -ne  "2" ]] ; then
 echo "You need to specify two arguments: first is the name of the sorted set, second is the number of items to put in it."
 exit 1
fi

for i in $(seq -f %f 1 $2) ;
do
    score=$i
    payload="payload_$i"
    echo "ZADD $1 $score $payload"
done