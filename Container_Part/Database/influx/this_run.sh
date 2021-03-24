#!/bin/bash

# read 할 csv 정보 및 redis 정보

# [1] target field name, 2개 이상의 필드 입력할때 공란없이 붙여서 | 로 표시함 
field="DRIVE_SPEED|DRIVE_LENGTH_TOTAL"
# [2] time field name
ts="RECORD_TIME"
# [3] id field name
carid="PHONE_NUM"
# [4] influxdb ip
ip="localhost"
# [5] influxdb port
port=8086
# [6] influxdb server username
username = 'cschae'
# [7] influxdb server password
password = 'evev2021'
# [8] influxdb databasename
database_name = 'test1'
# [9] influxdb measurement name
measurement_name = 'test1'


echo ">>===================================================="
echo "실행 관련 주요 정보(this_run.sh)"
echo "target field name  : "$field
echo "time field name   : "$ts
echo "id field name   : "$carid
echo "influxdb ip : "$ip
echo "influxdb port    : " $port
echo "influxdb user name : " $username 
echo "influxdb server password : " $password 
echo "influxdb database name : " $database_name 
echo "influxdb measurement name : " $measurement_name
echo "====================================================<<"

# 인자값 9개
#                              [1]   [2]  [3]    [4]  [5]  [6]       [7]       [8]            [9]
time python3 influx_put_data.py $field $ts $carid $ip $port $username $password $database_name $measurement_name

echo " *** end script run for PYTHON *** "