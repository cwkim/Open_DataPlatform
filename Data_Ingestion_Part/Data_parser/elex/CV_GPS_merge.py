# -*- coding:utf-8 -*-

""" 
    Author: Taeil Yun (KETI) / taeil710@gmail.com
    CV_GPS_merge.py: GPS 데이터 병합 코드
"""



from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
from datetime import datetime
import sys
from ast import literal_eval


# 몽고 DB 기본 클래스
class MongoDefault:
    def __init__(
        self,
        mo_address,
        mo_username,
        mo_password,
        mo_authsource,
        mo_database,
        mo_collection,
    ):

        # 쿼리해올 데이터가 있는 DB
        self.mo_address = mo_address  # 주소
        self.mo_username = mo_username  # 사용자이름
        self.mo_password = mo_password  # 비밀번호
        self.mo_authsource = mo_authsource  # 권한
        self.mo_database = mo_database  # DB이름
        self.mo_collection = mo_collection  # Collection이름

        # 데이터를 저장할 DB
        self.save_mo_address = ""  # 저장할 몽고 주소
        self.save_mo_port = ""  # 저장할 몽고 포트
        self.save_mo_username = ""  # 사용자이름
        self.save_mo_password = ""  # 비밀번호
        self.save_mo_database = ""  # DB이름
        self.save_collection_list = []  # 저장할 컬렉션 리스트

        # 저장할 데이터
        self.driving_car_list = []  # 운행차량 ID리스트
        self.store_car_list = []  # 축적차량 ID리스트
        self.data_count = 0  # 저장할 데이터 건 수 단위 row
        self.data_size = 0  # 저장할 데이터 사이즈 용량 단위 mb?

        # 이전 누적거리 현재 누적거리
        self.previous_accumulate_distance = 0
        self.current_accumulate_distance = 0

        # 저장할 데이터 딕셔너리
        self.input_data_dict = {}

        # geo 딕셔너리 코드
        self.geo_list_code = {
            "서울특별시": 0,
            "부산광역시": 1,
            "대구광역시": 2,
            "인천광역시": 3,
            "광주광역시": 4,
            "대전광역시": 5,
            "울산광역시": 6,
            "세종특별자치시": 7,
            "경기도": 8,
            "강원도": 9,
            "충청북도": 10,
            "충청남도": 11,
            "전라북도": 12,
            "전라남도": 13,
            "경상북도": 14,
            "경상남도": 15,
            "제주도": 16,
            "제주특별자치도": 16,
        }

    # 몽고 DB연결
    def connectmongodb(self):
        """mongodb 연결"""

        # 데이터를 가져올 몽고 연결
        self.Bring_data_mo_client = MongoClient(
            self.mo_address,
            username=self.mo_username,
            password=self.mo_password,
            authSource=self.mo_authsource,
            connect=False,
        )
        self.mo_db = self.Bring_data_mo_client.get_database(self.mo_database)
        print("----------------------------------------------------")
        print("Bring data MongoDB Client Connected")
        print("address:", str(self.mo_address))
        print("databse:", self.mo_database)
        print("----------------------------------------------------\n")

        # 데이터를 저장할 몽고 연결
        self.Input_data_mo_client = MongoClient(
            self.save_mo_address + ":" + self.save_mo_port,
            username=self.save_mo_username,
            password=self.save_mo_password,
            connect=False,
        )

        print("----------------------------------------------------")
        print("Input data MongoDB Client Connected")
        print("address:", str(self.save_mo_address + ":" + self.save_mo_port))
        print("databse:", self.save_mo_database)
        print("----------------------------------------------------\n")

    # umc 일별 데이터
    def Merge_Data(self):
        """"""

        # 변수 초기화
        self.driving_car_list = []

        print("----------------------------------------------------")
        print("merge data")

        # DAU 컬렉션 이름
        ELEX_GPS_Col = ""
        TRIPHOS_GPS_Col = ""
        merge_GPS_Col = ""

        ELEX_col = self.mo_db.get_collection(ELEX_GPS_Col)
        print("ELEX 컬렉션:", ELEX_col)

        TRIPHOS_col = self.mo_db.get_collection(TRIPHOS_GPS_Col)
        print("TRIPHOS 컬렉션:", TRIPHOS_col)

        # 커서
        # cursor_date = datetime(2019, 5, 1, 0, 0, 0, 125000)

        TRIPHOS_date = datetime(2019, 12, 31, 0, 0, 0, 125000)
        current_time = datetime.now()
        last_time = current_time + relativedelta(months=-1)
        cursor_date = datetime(last_time.year, last_time.month, 1, 0, 0, 0, 125000)

        while cursor_date < current_time:
            print(cursor_date)
            ELEX_Result = ELEX_col.find(
                {"datetime": cursor_date}, no_cursor_timeout=True
            )
            TRIPHOS_Result = TRIPHOS_col.find(
                {"datetime": cursor_date}, no_cursor_timeout=True
            )

            geo_list = []  # 0~16 까지 크기 17의 리스트
            # TRIPHOS_geo_list = [] # 0~16 까지 크기 17의 리스트

            for _ in range(17):
                geo_list.append(0)
                # TRIPHOS_geo_list.append(0)

            for geo_code in range(0, 17):
                geo_location_name = ""

                ELEX_Result.rewind()
                for ELEX_dict in ELEX_Result:

                    if ELEX_dict["geo_location_code"] == geo_code:

                        geo_list[geo_code] += ELEX_dict["drive_count"]
                        geo_location_name = ELEX_dict["geo_location"]
                        break
                TRIPHOS_Result.rewind()
                for TRIPHOS_dict in TRIPHOS_Result:

                    if TRIPHOS_dict["geo_location_code"] == geo_code:

                        geo_list[geo_code] += TRIPHOS_dict["drive_count"]
                        break

                if geo_location_name != "":
                    if cursor_date > TRIPHOS_date:
                        if geo_code == 0:  # 서울
                            geo_list[geo_code] += 200
                        elif geo_code == 1:  # 부산
                            geo_list[geo_code] += 50
                        elif geo_code == 3:  # 인천
                            geo_list[geo_code] += 150
                        elif geo_code == 4:  # 광주
                            geo_list[geo_code] += 50
                        elif geo_code == 5:  # 대전
                            geo_list[geo_code] += 50
                        elif geo_code == 8:  # 경기
                            geo_list[geo_code] += 1400
                        elif geo_code == 10:  # 충북
                            geo_list[geo_code] += 300
                        elif geo_code == 11:  # 충남
                            geo_list[geo_code] += 200
                        elif geo_code == 12:  # 전북
                            geo_list[geo_code] += 100
                        elif geo_code == 13:  # 전남
                            geo_list[geo_code] += 150
                        elif geo_code == 15:  # 경남
                            geo_list[geo_code] += 150

                    input_data_dict_1 = {
                        "datetime": ELEX_dict["datetime"],
                        "geo_location": geo_location_name,
                        "geo_location_code": geo_code,
                        "drive_count": geo_list[geo_code],
                    }
                    self.save_to_mongo("GPS_merge", input_data_dict_1)

            cursor_date = cursor_date + relativedelta(months=1)

    def save_to_mongo(self, collection_name, data_dict):

        # 컬렉션 설정
        db = self.Input_data_mo_client[self.save_mo_database]
        collection = db[collection_name]
        data = data_dict

        try:
            collection.insert_one(data)
            print("데이터 저장 확인")
            print("----------------------------------------------------")
            # logger.info("현재 데이터 저장 확인되었습니다.")

        except Exception as e:
            print("save error!")
            print(e)
            # logger.error(e)
            sys.exit(1)


if __name__ == "__main__":

    print("-----------------------------------------------------------------")
    print("Start /", "Time:", datetime.now())

    # global logger
    # logger = logs.get_logger(None, "./logs/CV_DAU.log")

    elex = MongoDefault(
        mo_address="",
        mo_username="",
        mo_password="",
        mo_authsource="",
        mo_database="",
        mo_collection=None,
    )
    elex.connectmongodb()
    elex.Merge_Data()
