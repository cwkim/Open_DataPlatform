#!/bin/bash

service ssh start

bash -c "while ! curl -s "db":27017 > /dev/null; do\
 echo waiting for mongodb; sleep 3;\
 done;"
 
bash ./this_run.sh

while true;
  do sleep 600;
done
