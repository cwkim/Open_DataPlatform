#!/bin/bash
# 파이션 코드 실행스크립트

echo " 실행 시작 : 파일명 "$0

# todo : 검색 대상 디렉토리를 입력 받는 코드 
# if $1 존재하면, $1을 입력 경로 변수로 저장
printf "\n검색 대상디렉토리 경로를 입력하세요\n (Enter 입력시 => ../files/JSON 적용) : "
read input1
if [ "${input1}" = '' ];then
    input1='../files/JSON'
fi
input_dir=$input1

printf "\n결과 파일이 저장될 대상디렉토리 경로를 입력하세요\n (Enter 입력시 => ../files 적용) : "
read input2
if [ "${input2}" = '' ];then
    input2='../files'
fi
output_dir=$input2

printf "\n새로운 파일로 저장 시 파일당 json 개수를 입력하세요\n(Enter 입력시 => 250000 적용) : "
read input3
if [ "${input3}" = '' ];then
    input3="250000"
fi
bundle=$input3

printf ""
echo ">>===================================================="
echo "실행 관련 주요 정보"
echo $MainCode $input_dir $bundle
echo "====================================================<<"

time python rmOutlier.py -input_dir $input_dir -output_dir $output_dir -bundle $bundle

echo " *** end script run for "