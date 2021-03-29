#!/bin/bash
# 파이션 코드 실행스크립트
# Author : Chaechulseung

python print_saved_var.py

MainCode='image_save.py'

printf "그래프에 그릴 메트릭 개수를 입력해주세요[1 or 2] : "
read input0
m_num=${input0}

printf "쿼리할 TSDB ip를 입력해주세요[이전 저장값 사용 : d 입력 // ex) 125.140.110.217] : "
read input1
ip=${input1}

printf "쿼리할 TSDB port를 입력해주세요[이전 저장값 사용 : d 입력 // ex) 54242] : "
read input2
port=${input2}

printf "쿼리할 TSDB metric를 입력해주세요[이전 저장값 사용 : d 입력 // 2개 인자값 입력시 '/'로 구분] : "
read input3
metric=${input3}

printf "쿼리 시작 날짜를 입력해주세요[이전 저장값 사용 : d 입력 // ex) 2019/06/01-00:00:00] : "
read input4
start=${input4}

printf "쿼리 끝 날짜를 입력해주세요[이전 저장값 사용 : d 입력 // ex) 2019/06/08-00:00:00] : "
read input5
end=${input5}

printf "쿼리할 field이름을 입력해주세요[이전 저장값 사용 : d 입력 // 2개 인자값 입력시 '/'로 구분] : "
read input6
field=${input6}

printf "이미지파일이 저장될 파일경로를 입력해주세요[기본경로 사용 : d 입력 // 기본경로 : './img/'] : "
read input7
img_filename=${input7}


printf ""
echo ">>===================================================="
echo "실행 관련 주요 정보"
echo $MainCode $ip $port $metric $start $end $field $img_filename
echo "====================================================<<"

# time 은 스크립트 SW 실행 시간을 확인하기 위해 사용
time python $MainCode -m_num $m_num -ip $ip -port $port -metric $metric -start $start -end $end -field $field -img_filename $img_filename

echo " *** end script run for PYTHON *** "
