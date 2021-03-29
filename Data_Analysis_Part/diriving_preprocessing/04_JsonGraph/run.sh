#!/bin/bash
# 파이션 코드 실행스크립트

printf "데이터를 새로 갱신할 경우 [n]을 기존 것을 사용하실 경우 [o]를 입력하세요 : "
read input0
renewal=${input0}

if [ ${renewal} = 'o' ];then
    python show_info.py $renewal
else
    printf "데이터가 있는 디렉토리 경로를 입력하세요\n"
    printf "(default값 : '../3_rmOutlier/cleanupData'을 입력하고 싶으면 [d]를 입력하세요 : "
    read input1
    dir=${input1}
    if [ ${dir} = 'd' ];then
        dir="../3_rmOutlier/cleanupData"
    fi
    python show_info.py $renewal $dir
fi

printf "그래프에서 비교할 VEHICLE_NUM의 번호를 2개 입력하세요(각 번호사이 ',' 사용)\n"
printf "(입력 예제 : 1,2) : "
read input2
carid=${input2}

printf "그래프에서 비교할 fieldname의 번호를 1개 입력하세요 : "
read input3
fieldname=${input3}

MainCode='json_graph.py'

printf ""
echo ">>===================================================="
echo "실행 관련 주요 정보"
echo $MainCode $carid $fieldname
echo "====================================================<<"

# time 은 스크립트 SW 실행 시간을 확인하기 위해 사용
time python $MainCode -carid $carid -fieldname $fieldname

echo " *** end script run for PYTHON *** "