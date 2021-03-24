#!/bin/bash

# mongodb 정보

# [1] target mongo host
ip=$MONGO_IP
# [2] targe mongo port
port=$MONGO_PORT
# [3] mongo user name
user=$MONGO_USER
# [4] mongo user password
pw=$MONGO_PASSWORD
# [5] target DB name
dbname=$MONGO_DB_NAME
# [6] target COLLECTION name
collname=$MONGO_COLL_NAME
# [7] target id column name
id_field_name=$ID_COLUMN
# [8] target id count num
id_count_num=$ID_QUERY_COUNT

echo ">>===================================================="
echo "실행 관련 주요 정보(this_run.sh)"
echo "target mongo host  : "$ip
echo "target mongo port   : "$port
echo "mongo user name    : "$user
echo "mongo user password : "$pw
echo "target db name   : "$dbname
echo "target collection name : "$collname
echo "target id column name : "$id_field_name
echo "target id count num : "$id_count_num
echo "====================================================<<"


# time 은 스크립트 SW 실행 시간을 확인하기 위해 사용
# 인자값 7개
#                   [1]   [2]   [3]     [4]   [5]
python MONGO2CSV.py -ip $ip -port $port -username $user -password $pw -db_name $dbname -collection_name $collname -id_field_name $id_field_name -id_count_num $id_count_num

echo " *** end script run for PYTHON *** "
