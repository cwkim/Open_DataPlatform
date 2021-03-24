#!/bin/bash

# read 할 csv 정보 및 redis 정보

# [1] target field name, 2개 이상의 필드 입력할때 공란없이 붙여서 | 로 표시함 
field="DRIVE_SPEED|DRIVE_LENGTH_TOTAL"
# [2] time field name
ts="RECORD_TIME"
# [3] redis ip
ip="127.0.0.1"
# [4] redis port
port=6379

echo ">>===================================================="
echo "실행 관련 주요 정보(this_run.sh)"
echo "target field name  : "$field
echo "time field name   : "$ts
echo "redis ip : "$ip
echo "redis port    : " $port
echo "====================================================<<"

# 인자값 4개
#                              [1]   [2]  [3]  [4]    
time python3 redis_put_data.py $field $ts $ip $port

echo " *** end script run for PYTHON *** "
