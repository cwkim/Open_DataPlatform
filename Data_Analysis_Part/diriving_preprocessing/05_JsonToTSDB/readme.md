## Json To TSDB

  - CSVJSON실행으로 생성된 JSON 파일을 DataBase에 입력하는 코드(현재는 openTSDB에만 입력하는 코드 작성됨)
  - 입력 : Json파일 / 출력 : Database
  - ※ 추후 다른 DB에도 적용할 수 있도록 수정예정

----
### DB 입력완료 데이터

  - DB종류 | DB URL | metric/table 이름 | 입력된 데이터 기간 | 입력내용 | 총 json파일 용량 | 소요시간 | Link
    :--: | :--: | :--: | :--: | :--: | :--: | :--: | :--:
    openTSDB | 125.140.110.217:54242 | Elex_data | 2019/06/01 ~ 2019/06/08 (7일) | 4대 차량의 속도, 누적주행거리, RPM 일주일치 데이터 | 1.41GB | 약 80분(로컬pc) | [Link](http://125.140.110.217:54242/#start=2019/06/01-00:00:00&end=2019/06/08-00:00:00&m=none:Elex_data&o=&yrange=%5B0:%5D&wxh=800x600&style=linespoint)
