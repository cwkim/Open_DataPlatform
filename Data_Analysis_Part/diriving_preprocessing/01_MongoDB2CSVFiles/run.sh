#!/bin/bash
# 파이션 코드 실행스크립트
# Author : Chaechulseoung

printf "쿼리할 DB server ip를 입력해주세요[ (default : '125.140.110.217') ※if you want to use defualt, input Enter ] : "
read input0
if [ "${input0}" == '' ];then
    input0='125.140.110.217'
fi
ip=${input0}

printf "쿼리할 DB server port를 입력해주세요[ (default : '27027') ※if you want to use defualt, input Enter ] : "
read input1
if [ "${input1}" == '' ];then
    input1='27027'
fi
port=${input1}

printf "쿼리할 DB server의 username을 입력해주세요[ (default : 'cschae') ※if you want to use defualt, input Enter ] : "
read input2
if [ "${input2}" == '' ];then
    input2='cschae'
fi
username=${input2}

printf "쿼리할 DB server의 password를 입력해주세요[ (default : 'cschae') ※if you want to use defualt, input Enter ] : "
read input3
if [ "${input3}" == '' ];then
    input3='cschae'
fi
password=${input3}

printf "쿼리할 MongoDB의 database이름을 입력해주세요[ (default : 'hanuri') ※if you want to use defualt, input Enter ] : "
read input4
if [ "${input4}" == '' ];then
    input4='hanuri'
fi
db_name=${input4}

printf "쿼리할 MongoDB의 collection이름을 입력해주세요[ (default : '201910') ※if you want to use defualt, input Enter ] : "
read input5
if [ "${input5}" == '' ];then
    input5='201910'
fi
collection_name=${input5}

printf "쿼리할 data의 id field 이름을 입력해주세요[ (default : 'PHONE_NUM') ※if you want to use defualt, input Enter ] : "
read input6
if [ "${input6}" == '' ];then
    input6='PHONE_NUM'
fi
id_field_name=${input6}

printf "쿼리할 id 개수를 입력해주세요(id별로 데이터 포인트 개수가 많은 순서대로))[ (default : 10) ※if you want to use defualt, input Enter ] : "
read input7
if [ "${input7}" == '' ];then
    input7=10
fi
id_count_num=${input7}

printf ""
echo ">>===================================================="
echo "실행 관련 주요 정보"
echo $ip $port $username $password $db_name $collection_name $id_field_name $id_count_num
echo "====================================================<<"

time python Mongo2CSV.py -ip $ip -port $port -username $username -password $password -db_name $db_name -collection_name $collection_name -id_field_name $id_field_name -id_count_num $id_count_num
echo " *** end script run for PYTHON *** "