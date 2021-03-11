# -*- coding:utf-8 -*-

""" 
    Author: Taeil Yun (KETI) / taeil710@gmail.com
    CV_TRIPHOS_MCD.py: TRIPHOS 월별 운행 횟수 수집 코드
"""

import pandas as pd
import pymysql
from datetime import timedelta, datetime
from pymongo import MongoClient
import sys
import json
from ast import literal_eval
import time

# 몽고 DB 기본 클래스
class MongoDB_Default:
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

      
        self.driving_car_list = []  # 운행차량 ID리스트
        self.store_car_list = []  # 축적차량 ID리스트
        self.date_time_month = datetime.now()  # 저장할 데이트 타임 월별
        self.date_time_day = datetime.now()  # 저장할 데이트 타임 일별
        self.data_count = 0  # 저장할 데이터 건 수 단위 row
        self.data_size = 0  # 저장할 데이터 사이즈 용량 단위 mb?

        # 이전 누적거리 현재 누적거리
        self.previous_accumulate_distance = 0
        self.current_accumulate_distance = 0

        self.input_data_dict = {}  # 저장할 데이터 딕셔너리

        self.dict_driving = {}  # 주행횟수 딕셔너리
        self.list_driving = []  # 주행횟수 리스트

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

        # self.mo_db2 = self.Input_data_mo_client.get_database(self.save_mo_database)

        print("----------------------------------------------------")
        print("Input data MongoDB Client Connected")
        print("address:", str(self.save_mo_address + ":" + self.save_mo_port))
        print("databse:", self.save_mo_database)
        print("----------------------------------------------------\n")

    # 트리포스 MCD 데이터
    def triphos_MCD(self):

        # 변수 초기화
        self.driving_car_list = []  # 변수 초기화가 필요함
        self.store_car_list = []  # 변수 초기화가 필요함

        print("----------------------------------------------------")
        print("triphos_MCD")

        # collection list 가져오기
        # collection_list = ['201906','201907','201908','201909','201910','201911', '201912']
        collection_list = self.mo_db.list_collection_names()

        collection_list.sort()
        # del collection_list[0]
        # del collection_list[0]
        print(collection_list)

        # car_list = []
        for col in collection_list:
            start = time.time()  # 시작 시간 저장

            # 변수 초기화
            self.driving_car_list = []  # 변수 초기화가 필요함
            self.store_car_list = []  # 변수 초기화가 필요함

            # 운행횟수 딕셔너리
            self.dict_driving = {}
            # 운행횟수 리스트
            self.list_driving = []
            for _ in range(31):
                self.list_driving.append(0)

 
            if "" in col:
                year = 
                month = 
                lastday = 
    

            print("Collection Name:", col)
            mo_col = self.mo_db.get_collection(col)
            print("컬렉션:", mo_col)

            # 1일부터 31일까지 하는게 필요함 월에따라서 변경이 필요 현재 테스트기때문에 1일 2일만했음
            for days in range(1, lastday + 1):  # 일별 for문

                # pymongo 시간쿼리 데이트타임으로 넣기
                self.date_time_day = datetime(year, month, days, 0, 0, 0, 125000)

                from_date = datetime(year, month, days, 5, 0, 0, 125000)
                to_date = datetime(year, month, days, 22, 0, 0, 125000)
                print("쿼리데이터 날짜", from_date)
                pipelines = list()
                pipelines.append(
                    {"$match": {"RECORD_TIME_KOR": {"$gte": from_date, "$lt": to_date}}}
                )
                pipelines.append(
                    {
                        "$group": {
                            "_id": {
                                "company_id": "$COMPANY_CD",
                                "car_id": "$VEHICLE_IDX",
                            }
                        }
                    }
                )
                data = mo_col.aggregate(pipelines)

                car_list2 = []
                for doc in data:
                    car_list2.append(str(doc))

                # 누적차량 운행차량
                self.driving_car_list = car_list2
                self.store_car_list += self.driving_car_list
                self.store_car_list = list(set(self.store_car_list))

                input_Driving_car_list = []
                input_Store_car_list = []

                for j in range(len(self.driving_car_list)):
                    dc = (
                        str(
                            literal_eval((self.driving_car_list[j]))["_id"][
                                "company_id"
                            ]
                        )
                        + "_"
                        + str(literal_eval((self.driving_car_list[j]))["_id"]["car_id"])
                    )
                    input_Driving_car_list.append(dc)
                    # input_Driving_car_list.append(literal_eval((self.driving_car_list[j]))['_id']['car_id'])

                for k in range(len(self.store_car_list)):
                    sc = (
                        str(literal_eval((self.store_car_list[k]))["_id"]["company_id"])
                        + "_"
                        + str(literal_eval((self.store_car_list[k]))["_id"]["car_id"])
                    )
                    input_Store_car_list.append(sc)
                    # input_Store_car_list.append(literal_eval((self.store_car_list[k]))['_id']['car_id'])

                # print("일별 count_driving_car : ",len(input_Driving_car_list),'대')
                # print("일별 count_store_car : ",len(input_Store_car_list),'대')

    
                if days == 1:
                    self.dict_driving = {string: 1 for string in input_Store_car_list}
                    # print('딕셔너리 : ',self.dict_driving)
                # 나머지 날
                else:
                    for l in range(len(input_Driving_car_list)):
                        if self.dict_driving.get(input_Driving_car_list[l]):
                            self.dict_driving[input_Driving_car_list[l]] += 1
                        else:
                            self.dict_driving.setdefault(input_Driving_car_list[l], 1)

                # print(self.dict_driving)

            for key in self.dict_driving.keys():
                self.list_driving[self.dict_driving[key] - 1] += 1

            # 월별 차량 운행횟수 저장 MCD
            for days2 in range(1, lastday + 1):
                self.input_data_dict = {
                    "datetime": self.date_time_day,
                    "drive_count": days2,
                    "drive_car": self.list_driving[days2 - 1],
                }
                self.save_to_mongo("TRIPHOS_MCD_200401~(new)")

            print("time :", time.time() - start)  # 현재시각 - 시작시간 = 실행 시간

            print(self.list_driving)
            

    # 몽고DB에 저장
    def save_to_mongo(self, collection_name):

        # 컬렉션 설정
        db = self.Input_data_mo_client[self.save_mo_database]
        collection = db[collection_name]
        data = self.input_data_dict

        try:
            collection.insert_one(data)
            # result = collection.insert_many(data)
            # result
            # print('%d rows are saved to "%s" collection in "%s" document successfully!' % (len(result.inserted_ids), '20190901', 'KST'))
            print("데이터 저장 확인")
            print("----------------------------------------------------")
            # sys.exit(1)
        except Exception as e:
            print("save error!")
            print(e)
            sys.exit(1)


if __name__ == "__main__":

    print(
        "-----------------------------------------------------------------------------------------------------------"
    )
    print("Start /", "Time:", datetime.now())

    """데이터 이전 인스턴스 생성"""
    triphos = MongoDB_Default(
        mo_address="",
        mo_username="",
        mo_password="",
        mo_authsource="",
        mo_database="",
        mo_collection=None,
    )

    triphos.connectmongodb()

    triphos.triphos_MCD()
