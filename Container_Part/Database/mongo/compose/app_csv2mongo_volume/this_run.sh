#!/bin/bash

# mongodb, csv 정보

# [1] target mongo host
ip=$MONGO_IP
# [2] targe mongo port
port=$MONGO_PORT
# [3] target DB name
dbname=$MONGO_DB_NAME
# [4] mongo user name
user=$MONGO_USER
# [5] mongo user password
pw=$MONGO_PASSWORD
# [6] csv time column name
time_col=$CSV_TIME_COL
# [7] csv id column name
id_col=$CSV_ID_COL

echo ">>===================================================="
echo "실행 관련 주요 정보(this_run.sh)"
echo "target mongo host  : "$ip
echo "target mongo port   : "$port
echo "target db name   : "$dbname
echo "mongo user name    : "$user
echo "mongo user password : "$pw
echo "csv time column name : "$time_col
echo "csv id column name : "$id_col
echo "====================================================<<"


# time 은 스크립트 SW 실행 시간을 확인하기 위해 사용
# 인자값 7개
#                   [1]   [2]   [3]     [4]   [5]
python CSV2MONGO.py -ip $ip -port $port -dbname $dbname -user $user -pw $pw -time_col $time_col -id_col $id_col

echo " *** end script run for PYTHON *** "