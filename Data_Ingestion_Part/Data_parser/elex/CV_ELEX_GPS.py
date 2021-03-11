# -*- coding:utf-8 -*-

""" 
    Author: Taeil Yun (KETI) / taeil710@gmail.com
    CV_ELEX_GPS.py: ELEX, UMC GPS 데이터 수집 코드
"""


import pandas as pd
import pymysql
from datetime import timedelta, datetime
from pymongo import MongoClient
import sys
import json
from ast import literal_eval
import requests


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
        self.list_driving = []  # 주행횟수 딕셔너리

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

        # self.mo_db2 = self.Input_data_mo_client.get_database(self.save_mo_database)

        print("----------------------------------------------------")
        print("Input data MongoDB Client Connected")
        print("address:", str(self.save_mo_address + ":" + self.save_mo_port))
        print("databse:", self.save_mo_database)
        print("----------------------------------------------------\n")

    # 일별 데이터
    def elexdistinct(self):

        # 변수 초기화
        self.driving_car_list = []  # 변수 초기화가 필요함
        self.store_car_list = []  # 변수 초기화가 필요함

        print("----------------------------------------------------")
        print("mongodistinct_days")

        # collection list 가져오기
        collection_list = [
            "",
        ]

        collection_list.sort()
        print(collection_list)

        # car_list = []
        for col in collection_list:

            if col == "":
                year = 
                month = 
                lastday = 

            # pymongo 시간쿼리 데이트타임으로 넣기
            self.date_time_day = datetime(year, month, 1, 0, 0, 0, 125000)

            print("Collection Name:", col)
            mo_col = self.mo_db.get_collection(col)
            print("컬렉션:", mo_col)

            pipelines = list()
            # pipelines.append({'$match': {'GPS_LAT': {'$exists': True}}})
            pipelines.append(
                {
                    "$match": {
                        "$and": [{"GPS_LAT": {"$exists": 1}}, {"GPS_LAT": {"$ne": 0}}]
                    }
                }
            )
            pipelines.append(
                {
                    "$group": {
                        "_id": {"car_id": "$PHONE_NUM"},
                        "avgGPS_LAT": {"$avg": "$GPS_LAT"},
                        "avgGPS_LONG": {"$avg": "$GPS_LONG"},
                    }
                }
            )
            data = mo_col.aggregate(pipelines)

            car_gps_list = []
            for doc in data:
                car_gps_list.append(str(doc))

            # 지역 카운트 세기 위한 딕셔너리, 리스트
            geo_count = {}
            geo_list = []

            for j in range(len(car_gps_list)):
                try:
                    # print('literal_eval_avgGPS_LAT : ',literal_eval(car_gps_list[j])['avgGPS_LAT'])
                    # print('literal_eval_avgGPS_Long : ',literal_eval(car_gps_list[j])['avgGPS_LONG'])
                    gps_y = str(literal_eval(car_gps_list[j])["avgGPS_LAT"])
                    gps_x = str(literal_eval(car_gps_list[j])["avgGPS_LONG"])
                    url = (
                        ""
                        + "x="
                        + gps_x
                        + "&y="
                        + gps_y
                    )
                    header = {
                        "authorization": "KakaoAK "
                    }
                    response = requests.get(url, headers=header)
                    geo_temp = json.loads(response.text).get("documents")[0][
                        "region_1depth_name"
                    ]

                    if geo_temp == "":
                        continue
                    else:
                        geo_list.append(
                            json.loads(response.text).get("documents")[0][
                                "region_1depth_name"
                            ]
                        )

                except:
                    pass

            for k in geo_list:
                try:
                    geo_count[k] += 1
                except:
                    geo_count[k] = 1

            print(geo_count)

            # 월별 차량 운행횟수 저장 GPS
            for key in geo_count.keys():
                self.input_data_dict = {}
                self.input_data_dict = {
                    "datetime": self.date_time_day,
                    "geo_location": key,
                    "geo_location_code": self.geo_list_code[key],
                    "drive_count": geo_count[key],
                }
                print("input_data_dict : ", self.input_data_dict)
                self.save_to_mongo("GPS")

            # 테스트니까 한번만 실행되게
            # break

    # umc 일별 데이터
    def umcdistinct(self):
        """"""

        # 변수 초기화
        self.driving_car_list = []  # 변수 초기화가 필요함
        self.store_car_list = []  # 변수 초기화가 필요함

        print("----------------------------------------------------")
        print("umcdistinct2_days")

        # collection list 가져오기
        collection_list = [
            "",
          
        ]
        # collection_list = ['mdt202007', 'mdt202008', 'mdt202009', 'mdt202010']
        collection_list.sort()
        print(collection_list)

        for col in collection_list:
            # col = 'mdt202007'   # 테스트기 때문에 가장 데이터가 적은 19년 09월 데이터로 설정
            if col == "":
                year = 
                month = 
                lastday = 
         
            # pymongo 시간쿼리 데이트타임으로 넣기
            self.date_time_day = datetime(year, month, 1, 0, 0, 0, 125000)

            print("Collection Name:", col)
            mo_col = self.mo_db.get_collection(col)
            print("컬렉션:", mo_col)

            pipelines = list()
            # pipelines.append({'$match': {'GPS_LAT': {'$exists': True}}})
            pipelines.append(
                {
                    "$match": {
                        "$and": [{"GPS_LAT": {"$exists": 1}}, {"GPS_LAT": {"$ne": 0}}]
                    }
                }
            )
            pipelines.append(
                {
                    "$group": {
                        "_id": {"car_id": "$PHONE_NUM"},
                        "avgGPS_LAT": {"$avg": "$GPS_LAT"},
                        "avgGPS_LONG": {"$avg": "$GPS_LONG"},
                    }
                }
            )
            data = mo_col.aggregate(pipelines)

            car_gps_list = []
            for doc in data:
                car_gps_list.append(str(doc))

            # 지역 카운트 세기 위한 딕셔너리, 리스트
            geo_count = {}
            geo_list = []

            for j in range(len(car_gps_list)):
                try:
                    # print('literal_eval_avgGPS_LAT : ',literal_eval(car_gps_list[j])['avgGPS_LAT'])
                    # print('literal_eval_avgGPS_Long : ',literal_eval(car_gps_list[j])['avgGPS_LONG'])
                    gps_y = str(literal_eval(car_gps_list[j])["avgGPS_LAT"])
                    gps_x = str(literal_eval(car_gps_list[j])["avgGPS_LONG"])
                    url = (
                        ""
                        + "x="
                        + gps_x
                        + "&y="
                        + gps_y
                    )

                    header = {
                        "authorization": "KakaoAK "
                    }
                    response = requests.get(url, headers=header)
                    geo_temp = json.loads(response.text).get("documents")[0][
                        "region_1depth_name"
                    ]

                    if geo_temp == "":
                        continue
                    else:
                        geo_list.append(
                            json.loads(response.text).get("documents")[0][
                                "region_1depth_name"
                            ]
                        )

                except:
                    pass

            for k in geo_list:
                try:
                    geo_count[k] += 1
                except:
                    geo_count[k] = 1

            print(geo_count)

            # 월별 차량 운행횟수 저장 GPS
            for key in geo_count.keys():
                self.input_data_dict = {}
                self.input_data_dict = {
                    "datetime": self.date_time_day,
                    "geo_location": key,
                    "geo_location_code": self.geo_list_code[key],
                    "drive_count": geo_count[key],
                }
                print("input_data_dict : ", self.input_data_dict)
                self.save_to_mongo("GPS")

            # 테스트니까 한번만 실행되게
            # break

        # 몽고DB에 저장

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
    elex = MongoDB_Default(
        mo_address="",
        mo_username="",
        mo_password="",
        mo_authsource="",
        mo_database="",
        mo_collection=None,
    )

    elex.connectmongodb()

    # 일별
    elex.elexdistinct()

    elex.mo_database = "umc"
    elex.connectmongodb()

    # umc 일별
    elex.umcdistinct()
