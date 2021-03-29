## rmOutlier.py

#### 입력받은 디렉토리의 json data (4대 차량의 RPM, DRIVE_SPEED, DRIVE_LENGTH_TOTAL)중 이상치를 제거해주고 제거된 데이터와 이상치 데이터 각각을 json파일로 만들어 주는 코드
----
### 코드 실행 내용
- 코드 실행시 사용자가 입력한 디렉토리에서 json파일을 가져와 각 필드에 따라 이상치를 찾고 그 데이터를 제거해 주고 필드명도 '기존_필드명_1'로 비꿔준다. 그 후 이상치가 제거된 데이터를 다시 json파일(./cleanupData)로 만들어줌.
- 이상치 데이터도 필드명을 '기존_필드명_OUT_1'로 바꿔주고 json파일(./outlierData)로 만들어줌.
- 실행 방법 : ./run.sh

----
#### 실행 결과
이상치가 제거된 후 json 파일(4대 차량의 RPM, DRIVE_SPEED, DRIVE_LENGTH_TOTAL)의 정보 요약
- [링크](./mdfile/logs.txt)

이상치로 판명된 json 파일
- [링크](./mdfile/logs2.txt)

----
#### 이상치 제거된 후의 json 파일 가져오는 법
<code>rsync -avhz --progress --partial -e 'ssh -p 7776' 본인ID@125.140.110.217:/home/data/weekjsondata/cleanupData ./</code>

----
#### OpenTSDB 쿼리결과
이상치 제거 후
- fieldanme : DRIVE_LENGTH_TOTAL_1
- [링크](http://125.140.110.217:54242/#start=2019/06/01-00:00:00&end=2019/06/08-00:00:00&m=none:Elex_data%7Bfieldname=DRIVE_LENGTH_TOTAL_1%7D&o=&yrange=%5B0:%5D&wxh=800x600&style=linespoint)
- fieldanme : DRIVE_SPEED_1
- [링크](http://125.140.110.217:54242/#start=2019/06/01-00:00:00&end=2019/06/08-00:00:00&m=none:Elex_data%7Bfieldname=DRIVE_SPEED_1%7D&o=&yrange=%5B0:%5D&wxh=800x600&style=linespoint)
- fieldanme : RPM_1
- [링크](http://125.140.110.217:54242/#start=2019/06/01-03:10:42&end=2019/06/03-21:00:58&m=none:Elex_data%7Bfieldname=RPM_1%7D&o=&yrange=%5B0:%5D&wxh=800x600&style=linespoint)
- total
- [링크](http://125.140.110.217:54242/#start=2019/06/01-00:00:00&end=2019/06/08-00:00:00&m=none:Elex_data%7Bfieldname=DRIVE_LENGTH_TOTAL_1,fieldname=DRIVE_SPEED_1,fieldname=RPM_1%7D&o=&yrange=%5B0:%5D&wxh=800x600&style=linespoint)

이상치만 따로 쿼리한 결과
- fieldname : DRIVE_LENGTH_TOTAL_1_OUT
- [링크](http://125.140.110.217:54242/#start=2019/06/01-00:00:00&end=2019/06/08-00:00:00&m=none:Elex_data%7Bfieldname=DRIVE_LENGTH_TOTAL_1_OUT%7D&o=&yrange=%5B0:%5D&wxh=800x600&style=linespoint)
- fieldname : RPM_1_OUT
- [링크](http://125.140.110.217:54242/#start=2019/06/01-00:00:00&end=2019/06/08-00:00:00&m=none:Elex_data%7Bfieldname=RPM_1_OUT%7D&o=&yrange=%5B0:%5D&wxh=800x600&style=linespoint)

-----

추가사항 

- 이상치로 판명된 데이터 따로 파일로 저장 (적용)
- 쉘 스크립트 입력부분 디폴트값 적용 (적용)
- 디렉토리 경로 수정 (../) (적용)



