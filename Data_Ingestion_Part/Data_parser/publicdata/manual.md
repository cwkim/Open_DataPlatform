## 1. TASS KOROAD (도로교통공단 교통사고정보 개방시스템)  
  
### 1. CSV 파일 URL 경로  
CSV 파일명과 다운로드된 파일명 연결 정보 (다운로드 파일은 files 디렉토리의 하위에 저장됨)  
"보행자무단횡단사고.csv": "pdestrians_jaywalking.csv"  
"스쿨존내어린이사고.csv": "schoolzone_child.csv"  
"보행어린이사고.csv": "child.csv"  
"자전거사고.csv": "bicycle.csv"  
"보행노인사고.csv": "oldman.csv"  
"사망교통사고.csv": "death.csv"  
  
#### ※ 다운로드 경로  
"pdestrians_jaywalking.csv":"http://taas.koroad.or.kr/api/down/jaywalkingdown.jsp"  
"schoolzone_child.csv":"http://taas.koroad.or.kr/api/down/schoolzonedown.jsp"  
"child.csv":"http://taas.koroad.or.kr/api/down/childdown.jsp"  
"bicycle.csv":"http://taas.koroad.or.kr/api/down/bicycledown.jsp"  
"oldman.csv":"http://taas.koroad.or.kr/api/down/oldmandown.jsp"  
"death.csv":"http://taas.koroad.or.kr/api/down/deathdown.jsp"  
  
### 2. CSV 파일의 Column 명과 MongoDB Column 명 연결 정보 (순서대로 일치함) 
#### For pdestrians_jaywalking.csv, schoolzone_child.csv, child.csv, bicycle.csv, oldman.csv  
CSV 파일의 Column 명 = ["다발지식별자", "다발지그룹식별자", "법정동코드", "스팟코드", "관할경찰서","다발지명", "발생건수", "사상자수", "사망자수", "중상자수", "경상자수", "부상신고자수", "경도", "위도", "다발지역폴리곤"]  
MongoDB Column 명 = ["ID", "DATE", "DISTRICT_CODE", "SPOT_CODE", "PRECINCT", "LOCATION", "NUM_OCCUR", "NUM_CASUALTY", "NUM_DEATH", "NUM_SEVERE", "NUM_SLIGHTLY", "NUM_INJURY", "GPS_LONG", "GPS_LAT", "POLYGON"]  
  
#### For death.csv  
CSV 파일의 Column 명 = ["발생년", "발생년월일시", "발생분", "주야", "요일", "사망자수", "사상자수","중상자수", "경상자수", "부상신고자수", "발생지시도", "발생지시군구", "사고유형_대분류", "사고유>형_중분류", "사고유형", "법규위반_대분류", "법규위반", "도로형태_대분류", "도로형태", "당사자종별_1당_대분류", "당사자종별_1당", "당사자종별_2당_대분류", "당사자종별_2당", "발생위치X_UTMK", "발>생위치Y_UTMK", "경도", "위도"]  
MongoDB Column 명 = ["YEAR", "DATETIME", "MINUTES", "DAY_NIGHT", "DAY", "NUM_DEATH", "NUM_CASUALTY", "NUM_SEVERE", "NUM_SLIGHTLY", "NUM_INJURY", "SIDO", "GUGUN", "L_ACCITYPE", "M_ACCITYPE", "S_ACCITYPE", "L_VIOLTYPE", "S_VIOLTYPE", "L_ROADTYPE", "S_ROADTYPE", "L_CARTYPE1", "S_CARTYPE1", "L_CARTYPE2", "S_CARTYPE2", "X_UTMK", "Y_UTMK", "GPS_LONG", "GPS_LAT"]  
  
##### ※ MongoDB 저장 시 pdestrians_jaywalking.csv, schoolzone_child.csv, child.csv, bicycle.csv, oldman.csv에서 삭제되는 Column 명: ["ID", "DISTRICT_CODE", "SPOT_CODE", "POLYGON"]  
##### ※ MongoDB 저장 시 death.csv에서 삭제되는 Column 명: ["YEAR", "MINUTES"]   
  
## 2. TS (한국교통안전공단)  
  
### 1. CSV 파일 저장 디렉토리 경로  
connected_car/src/data_parser/publicdata/ts_files 내 파일을 저장  
※ Zip 파일의 경우 zip 파일을 압축풀어 모든 csv 파일에 대해서 MongoDB에 저장 (현재 16개의 CSV 파일 저장함.) 
  
### 2. CSV 파일의 Column 명은 한글과 한자의 혼용으로 번역이 어렵기 때문에, 한글을 읽은 발음으로 Column 명을 사용  
ex) "나라사랑" : "nalasalang"  

