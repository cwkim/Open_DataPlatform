#!/bin/bash
# 파이션 코드 실행스크립트
# Author : Chaechulseoung

printf "원하는 타겟(output) DB 종류를 입력해주세요[ select (ots, influx, redis, couch), openTSDB, influxDB, RedisDB, Couchbase. Enter 입력시 : ots ] : "
read input0
if [ "${input0}" = '' ];then
    input0='ots'
fi
dbtype=${input0}

printf "입력(input)파일 JSON 위치를 입력해주세요(Enter입력시 : ../files/cleanupData) : "
read input1
if [ "${input1}" = '' ];then
    input1='../files/cleanupData'
fi
input=${input1}

printf "input DB URL을 입력해주세요(Enter입력시 : http://125.140.110.217:54242) : "
read input2
if [ "${input2}" = '' ];then
    input2='http://125.140.110.217:54242'
fi
url=${input2}

printf ""
echo ">>===================================================="
echo "실행 관련 주요 정보"
echo $dbtype $input $url
echo "====================================================<<"

# time 은 스크립트 SW 실행 시간을 확인하기 위해 사용 
time python JsonToTSDB.py -debug -dbtype $dbtype -input $input -url $url
echo " *** end script run for PYTHON *** "