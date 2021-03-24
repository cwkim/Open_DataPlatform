#!/bin/bash

# MARIAdb 정보

# [1] target maria host
ip=$MARIA_IP
# [2] targe maria port
port=$MARIA_PORT
# [3] maria user name
user=$MARIA_USER
# [4] maria user password
pw=$MARIA_PASSWORD
# [5] target or want to make DB name
dbname=$MARIA_DB_NAME
# [6] target or want to make table name
tablename=$MARIA_TABLE_NAME

echo ">>===================================================="
echo "실행 관련 주요 정보(this_run.sh)"
echo "target maria host  : "$ip
echo "target maria port   : "$port
echo "maria user name    : "$user
echo "maria user password : "$pw
echo "target or want to make DB name   : "$dbname
echo "target or want to make table name : "$tablename
echo "====================================================<<"


# time 은 스크립트 SW 실행 시간을 확인하기 위해 사용
# 인자값 7개   
#                   [1]     [2]         [3]         [4]     [5]             [6]
python CSV2MARIA.py -ip $ip -port $port -user $user -pw $pw -dbname $dbname -tablename $tablename

echo " *** end script run for PYTHON *** "
