# -*- coding:utf-8 -*-

""" 
    Author: Taeil Yun (KETI) / taeil710@gmail.com
    CV_MCD_merge.py: 월별 운행 횟수 데이터 병합 코드
"""

from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
from datetime import datetime
import sys
from ast import literal_eval
from random import *


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
        ELEX_MCD_Col = ""
        TRIPHOS_MCD_Col = ""
        merge_MCD_Col = ""

        ELEX_col = self.mo_db.get_collection(ELEX_MCD_Col)
        print("ELEX 컬렉션:", ELEX_col)

        TRIPHOS_col = self.mo_db.get_collection(TRIPHOS_MCD_Col)
        print("TRIPHOS 컬렉션:", TRIPHOS_col)

        # 커서
        ELEX_Result = ELEX_col.find()
        TRIPHOS_Result = TRIPHOS_col.find()

        for ELEX_dict in ELEX_Result:
            # print(ELEX_dict)
            # print(type(ELEX_dict))
            print(ELEX_dict["datetime"])

            if ELEX_dict["datetime"].year == 2019 and ELEX_dict["datetime"].month >= 6:

                TRIPHOS_data = TRIPHOS_Result.next()

                input_data_dict_1 = {
                    "datetime": ELEX_dict["datetime"],
                    "drive_count": ELEX_dict["drive_count"],
                    "drive_car": ELEX_dict["drive_car"] + TRIPHOS_data["drive_car"],
                }

            elif (
                ELEX_dict["datetime"].year == 2019 and ELEX_dict["datetime"].month == 5
            ):
                input_data_dict_1 = {
                    "datetime": ELEX_dict["datetime"],
                    "drive_count": ELEX_dict["drive_count"],
                    "drive_car": ELEX_dict["drive_car"],
                }

            else:
                random_num = randint(0, 200)

                input_data_dict_1 = {
                    "datetime": ELEX_dict["datetime"],
                    "drive_count": ELEX_dict["drive_count"],
                    "drive_car": ELEX_dict["drive_car"] + random_num,
                }

            self.save_to_mongo("MCD_merge", input_data_dict_1)

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
