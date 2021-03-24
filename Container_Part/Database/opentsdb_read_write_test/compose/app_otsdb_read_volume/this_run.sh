#!/bin/bash

# 파이션 코드 실행스크립트

echo " 시작날짜, 끝날짜는 최소 하루단위로 지정해야함. 최소 24시간 이상 (날짜 뒤에 시간은 -00:00:00으로 해줘야한다) "

# read 할 open tsdb 정보
# [1] 쿼리할 opentsdb ip
url_in=$TSDB_IP
# [2] 쿼리할 opentsdb port
port_in=$TSDB_PORT
# [3] 쿼리할 opentsdb metric명
metric_in=$TSDB_METRIC
# [4] 쿼리할 opentsdb 시작 시간
time_start=$QUERY_TIME_START
# [5] 쿼리할 opentsdb 끝 시간
time_end=$QUERY_TIME_END
# [6] # 쿼리할 차량 번호 ('none' 입력시 전체 차량 쿼리, '|' 문자로 여러 차량 쿼리 가능)
id=$CAR_ID
# [7] 쿼리할 전체기간 중 나눠 쿼리할 단위 ('d' = days, 'h' = hours, 'm' = minutes)
timeunit=$QUERY_TIME_UNIT
# [8] 쿼리할 전체기간 중 나눠 쿼리할 단위의 크기 (timeunit='d', timelong='7' => 7days)
timelong=$QUERY_TIME_LONG


echo ">>===================================================="
echo "실행 관련 주요 정보"
echo $url_in $port_in $metric_in $time_start $time_end $id $timeunit $timelong
echo "====================================================<<"

# time 은 스크립트 SW 실행 시간을 확인하기 위해 사용 
# 인자값 18개 
#                   
time python READ_TSDB.py $url_in $port_in $metric_in $time_start $time_end $id $timeunit $timelong

echo " *** end script run for PYTHON *** "