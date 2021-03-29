#!/bin/bash
# 파이션 코드 실행스크립트
# Author : Chaechulseoung

printf "원하는 타겟(output) DB 종류를 입력해주세요[ select (ots, influx, redis, couch), openTSDB, influxDB, RedisDB, Couchbase ] : "
read input0
dbtype=${input0}

printf "입력(input)파일 JSON 위치를 입력해주세요[ (Defualt : ../CSVJSON/output/resultData) ※if you want to use Defualt, write 'def' ] : "
read input1
input=${input1}

printf "input DB URL을 입력해주세요[ (Default : http://125.140.110.217:54242) ※if you want to use Defualt, write 'def' ] : "
read input2
url=${input2}

printf "DB에 put을 수행할 서브프로세스 개수를 입력하세요 : "
read input3
cn=${input3}



printf ""
echo ">>===================================================="
echo "실행 관련 주요 정보"
echo $dbtype $input $url $cn
echo "====================================================<<"

# time 은 스크립트 SW 실행 시간을 확인하기 위해 사용 
time python JsonToTSDB.py -debug -dbtype $dbtype -input $input -url $url -cn $cn
echo " *** end script run for PYTHON *** "