#!/bin/bash
# 파이션 코드 실행스크립트
# version 1.2
# Author : Chaechulseung
#          https://github.com/jeonghoonkang

echo " 실행 시작 : 파일명 "$0

# todo : 검색 대상 디렉토리를 입력 받는 코드 
# if $1 존재하면, $1을 입력 경로 변수로 저장
printf "\n검색 대상디렉토리 경로를 입력하세요 \n\
([d] 입력시 default 값(./inputfile)을 적용합니다) : "
read input1
input_dir=${input1}
if [ $input_dir == 'd' ]
then
  input_dir='./inputfile'
fi

printf "\n기존 Json 요약 정보 파일(__JsonDataInfo.py)이 있을시\n\
그대로 사용하려면 [y] 입력, 새로운 파일로 만들고 싶으면 [n] 입력 : "
read input2
overwrite=${input2}

printf "\n"
echo "검색 대상 디렉토리 경로 : "$input_dir
echo "기존 파일 사용 여부 : "$overwrite
#time python InformJson.py $input_dir
python InformJson.py $input_dir $overwrite

echo "---------- waiting for input ----------" 
printf "요약 정보를 볼 JsonData Index를 입력하세요 \n\
([a] 를 입력하면 모든 종류에 대해 처리합니다, [n] 은 종료) : "

read input3
index=${input3}

if [ $index == 'n' ] #쉘 스크립트에서 문자열 비교는 == 사용  
then
  exit
fi

echo ""
echo "---------- JSON key, value 요약 정보 추출 ----------" 
# time python showJson.py $index
python showJson.py $index

echo " *** end script run for "$0
