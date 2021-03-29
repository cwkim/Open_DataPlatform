
- EXCELtoCSV.py 
  - 현재 디렉토리와 서브디렉토리를 탐색하여 모든 xlsx파일을 모은 후 csv파일로 변환해주는 코드
  - 변환된 csv파일은 csvoutput 디렉토리에 생성된다.
- 단, xlsx파일 read시 시간이 오래 걸리기 떄문에 다수의 xlsx파일 변환 시 run time이 매우 길어질 것이다.
  - MultiProcess를 적용하여 5개의 Process가 각각 XLSX 파일 1개씩 CSV 파일로 변환해줌
