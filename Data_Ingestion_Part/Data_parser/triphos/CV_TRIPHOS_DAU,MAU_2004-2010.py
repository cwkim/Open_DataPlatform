# -*- coding:utf-8 -*-

""" 
    Author: Taeil Yun (KETI) / taeil710@gmail.com
    CV_TRIPHOS_DAU,MAU_2004-2010.py: TRIPHOS 20년 4월 ~ 20년 10월 까지 일별, 월별 데이터 수집 코드
"""

import sys
import pandas as pd
import pymysql
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
import sys
import json
from ast import literal_eval


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
        print("databse_list:", self.Bring_data_mo_client.list_database_names())
        print("databse:", self.mo_database)
        print("col_list:", self.mo_db.list_collection_names())
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

    # 트리포스 일별 데이터
    def triphosdistinct(self):

        # 변수 초기화
        self.driving_car_list = []  # 변수 초기화가 필요함
        self.store_car_list = []  # 변수 초기화가 필요함

        print("----------------------------------------------------")
        print("triphosdistinct_days")

        # collection list 가져오기 # 202006 기준으로 날짜 데이터가 안들어옴
        # collection_list = ['201906','201907','201908','201909','201910','201911','201912']
        # collection_list = ['ROUTE_LOG_202004','ROUTE_LOG_202005','ROUTE_LOG_202006','ROUTE_LOG_202007','ROUTE_LOG_202008','ROUTE_LOG_202009','ROUTE_LOG_202010']
        collection_list = self.mo_db.list_collection_names()

        # collection_list2 = ['202001','202002','202003','202004','202005'] # 바뀐거
        collection_list.sort()
        print(collection_list)

        for col in collection_list:

            if "" in col:
                year = 
                month = 
                lastday = 
      

            print("Collection Name:", col)
            mo_col = self.mo_db.get_collection(col)
            print("컬렉션:", mo_col)

            for days in range(1, lastday + 1):  # 일별 for문

                # 아예없으면
                if year == 2020 and month == 4 and days == 1:
                    pass

                else:

                    carlist_db = self.Input_data_mo_client[self.save_mo_database]
                    RCL_collection = carlist_db.get_collection(
                        "TRIPHOS_RCL_D_200401~(new)"
                    )

                    car_list_from_date = datetime(
                        year, month, days, 0, 0, 0, 0
                    ) + timedelta(days=-1)
                    car_list_to_date = datetime(
                        year, month, days, 22, 0, 0, 0
                    ) + timedelta(days=-1)

                    car_list_data = list(
                        RCL_collection.find(
                            {
                                "datetime": {
                                    "$gte": car_list_from_date,
                                    "$lt": car_list_to_date,
                                }
                            }
                        )
                    )

                    # 축적차량 ID리스트
                    self.store_car_list = car_list_data[0].get("list_store_car")

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
                # pipelines.append({'$group': {'_id': {'car_id': '$PHONE_NUM'}}})
                data = mo_col.aggregate(pipelines)

                car_list2 = []
                for doc in data:
                    car_list2.append(str(doc))
                    print(doc)
                    # print("check")

                # 수집데이터량을 계산하기 위한 count
                # pipelines.append({'$count: data_count'})
                datacount = mo_col.count_documents(
                    {"RECORD_TIME_KOR": {"$gte": from_date, "$lt": to_date}}
                )
                datasize = (datacount * 86) / 1000000  # 단위 MB
                print("일별 datacount : ", datacount)
                print("일별 datasize : ", datasize, "mb")

                # 누적 데이터량 계산
                if year == 2020 and month == 4 and days == 1:
                    accumulate_data_count = datacount
                    accumulate_data_size = datasize
                else:

                    accum_data_db = self.Input_data_mo_client[self.save_mo_database]
                    accum_data_collection = accum_data_db.get_collection(
                        "TRIPHOS_DAU_200401~(new)"
                    )

                    accum_data_from_date = datetime(
                        year, month, days, 0, 0, 0, 0
                    ) + timedelta(days=-1)
                    accum_data_to_date = datetime(
                        year, month, days, 22, 0, 0, 0
                    ) + timedelta(days=-1)

                    accum_data_data = list(
                        accum_data_collection.find(
                            {
                                "datetime": {
                                    "$gte": accum_data_from_date,
                                    "$lt": accum_data_to_date,
                                }
                            }
                        )
                    )

                    accumulate_data_count = (
                        accum_data_data[0].get("accumulate_data_count") + datacount
                    )
                    accumulate_data_size = (
                        accum_data_data[0].get("accumulate_data_size") + datasize
                    )

                self.driving_car_list = car_list2
                self.store_car_list += self.driving_car_list
                self.store_car_list = list(set(self.store_car_list))

                input_Driving_car_list = []
                input_Store_car_list = []

                for j in range(len(self.driving_car_list)):
                    company_id = literal_eval(self.driving_car_list[j])["_id"][
                        "company_id"
                    ]
                    car_id = literal_eval(self.driving_car_list[j])["_id"]["car_id"]
                    input_Driving_car_list.append(str(company_id) + "-" + str(car_id))

                for k in range(len(self.store_car_list)):
                    company_id = literal_eval(self.store_car_list[k])["_id"][
                        "company_id"
                    ]
                    car_id = literal_eval(self.store_car_list[k])["_id"]["car_id"]
                    input_Store_car_list.append(str(company_id) + "-" + str(car_id))

                print("일별 count_driving_car : ", len(input_Driving_car_list), "대")
                print("일별 count_store_car : ", len(input_Store_car_list), "대")

                # DAU2 저장
                self.input_data_dict = {}
                self.input_data_dict = {
                    "datetime": self.date_time_day,
                    "count_driving_car": len(input_Driving_car_list),
                    "count_store_car": len(input_Store_car_list),
                    "data_count": datacount,
                    "accumulate_data_count": accumulate_data_count,
                    "data_size": datasize,
                    "accumulate_data_size": accumulate_data_size,
                }

                self.save_to_mongo("TRIPHOS_DAU_200401~(new)")

                # RCL에 저장할거
                self.input_data_dict = {}
                self.input_data_dict = {
                    "datetime": self.date_time_day,
                    "list_driving_car": self.driving_car_list,
                    "list_store_car": self.store_car_list,
                }
                self.save_to_mongo("TRIPHOS_RCL_D_200401~(new)")
                """
                """

               

        # 몽고DB에 저장

    # 트리포스 월별 데이터
    def triphosdistinct2(self):

        # 변수 초기화
        self.driving_car_list = []  # 운행차량 ID리스트
        self.store_car_list = []  # 축적차량 ID리스트

        print("----------------------------------------------------")
        print("elexdistinct2_month")

        # collection list 가져오기 # 202006 기준으로 날짜 데이터가 안들어옴
        # collection_list = ['201906','201907','201908','201909','201910','201911','201912']
        collection_list = self.mo_db.list_collection_names()
        del collection_list[0]

        collection_list.sort()
        print(collection_list)

        for col in collection_list:

            if "" in col:
                year = 
                month = 
                lastday = 
     

            if year == 2020 and month == 4:
                pass
            else:

                carlist_db = self.Input_data_mo_client[self.save_mo_database]
                RCL_collection = carlist_db.get_collection("TRIPHOS_RCL_M_200401~(new)")

                car_list_from_date = datetime(
                    year, month, 1, 0, 0, 0, 0
                ) + relativedelta(months=-1)
                car_list_to_date = datetime(
                    year, month, 1, 22, 0, 0, 0
                ) + relativedelta(months=-1)

                car_list_data = list(
                    RCL_collection.find(
                        {
                            "datetime": {
                                "$gte": car_list_from_date,
                                "$lt": car_list_to_date,
                            }
                        }
                    )
                )

                # 축적차량 ID리스트
                self.store_car_list = car_list_data[0].get("list_store_car")

            # pymongo 시간쿼리 데이트타임으로 넣기
            self.date_time_month = datetime(year, month, 1, 0, 0, 0, 125000)

            print("Collection Name:", col)
            mo_col = self.mo_db.get_collection(col)
            print("컬렉션:", mo_col)

            pipelines2 = list()
            pipelines2.append(
                {
                    "$group": {
                        "_id": {"company_id": "$COMPANY_CD", "car_id": "$VEHICLE_IDX"}
                    }
                }
            )
            data2 = mo_col.aggregate(pipelines2)

            car_list3 = []
            for doc2 in data2:
                car_list3.append(str(doc2))
                # print(doc2)

            self.driving_car_list = car_list3
            self.store_car_list += self.driving_car_list
            self.store_car_list = list(set(self.store_car_list))

            # 누적거리를 계산하기 위한 aggregate
            pipelines = list()
            pipelines.append(
                {
                    "$match": {
                        "$and": [
                            {"DRIVE_LENGTH_DAY": {"$ne": "0"}},
                            {"DRIVE_LENGTH_DAY": {"$exists": 1}},
                        ]
                    }
                }
            )
            pipelines.append(
                {
                    "$group": {
                        "_id": {
                            "company_id": "$COMPANY_CD",
                            "car_id": "$VEHICLE_IDX",
                            "max_len": {"$max": "$DRIVE_LENGTH_DAY"},
                            "min_len": {"$min": "$DRIVE_LENGTH_DAY"},
                        }
                    }
                }
            )

            data = mo_col.aggregate(pipelines, allowDiskUse=True)

            # 누적 거리 계산
            for doc in data:
                self.current_accumulate_distance += doc["_id"]["max_len"]

            # 이전달 누적거리를 빼고 이전달 누적거리에 저장
            self.current_accumulate_distance -= self.previous_accumulate_distance
            self.previous_accumulate_distance = self.current_accumulate_distance

            # 수집데이터량을 계산하기 위한 count
            from_date = datetime(year, month, 1, 5, 0, 0, 125000)
            to_date = datetime(year, month, lastday, 22, 0, 0, 125000)
            print("from_date : ", from_date)
            print("from_date : ", type(from_date))
            print("to_date : ", to_date)
            # datacount = mo_col.count_documents({'RECORD_TIME': {"$gte": from_date,"$lt": to_date}})
            datacount = mo_col.count_documents(
                {"RECORD_TIME_KOR": {"$gte": from_date, "$lt": to_date}}
            )
            # dummy = mo_col.find().limit(10)
            # dummy = pd.DataFrame(dummy)
            # print('find : ', dummy.columns)

            datasize = (datacount * 86) / 1000000  # 단위 MB

            # 누적 데이터량 계산
            if year == 2020 and month == 4:
                accumulate_data_count = datacount
                accumulate_data_size = datasize
            else:

                accum_data_db = self.Input_data_mo_client[self.save_mo_database]
                accum_data_collection = accum_data_db.get_collection(
                    "TRIPHOS_MAU_200401~(new)"
                )

                accum_data_from_date = datetime(
                    year, month, 1, 0, 0, 0, 0
                ) + relativedelta(months=-1)
                accum_data_to_date = datetime(
                    year, month, 1, 22, 0, 0, 0
                ) + relativedelta(months=-1)

                accum_data_data = list(
                    accum_data_collection.find(
                        {
                            "datetime": {
                                "$gte": accum_data_from_date,
                                "$lt": accum_data_to_date,
                            }
                        }
                    )
                )

                accumulate_data_count = (
                    accum_data_data[0].get("accumulate_data_count") + datacount
                )
                accumulate_data_size = (
                    accum_data_data[0].get("accumulate_data_size") + datasize
                )

            print("월별 datacount : ", datacount)
            print("월별 datasize : ", datasize, "mb")
            print("월별 누적 datacount : ", accumulate_data_count)
            print("월별 누적 datasize : ", accumulate_data_size, "mb")

            print("월별 count_driving_car : ", len(self.driving_car_list), "대")
            print("월별 count_store_car : ", len(self.store_car_list), "대")
            print(
                "월별 current_accumulate_distance : ",
                self.current_accumulate_distance,
                "m",
            )  # 단위 m

            # MAU 저장
            self.input_data_dict = {
                "datetime": self.date_time_month,
                "count_driving_car": len(self.driving_car_list),
                "count_store_car": len(self.store_car_list),
                "data_count": datacount,
                "accumulate_data_count": accumulate_data_count,
                "data_size": datasize,
                "accumulate_data_size": accumulate_data_size,
                "accumulate_distance": self.current_accumulate_distance,
            }
            self.save_to_mongo("TRIPHOS_MAU_200401~(new)")

            # RCL에 저장할거
            self.input_data_dict = {}
            self.input_data_dict = {
                "datetime": self.date_time_month,
                "list_driving_car": self.driving_car_list,
                "list_store_car": self.store_car_list,
            }
            self.save_to_mongo("TRIPHOS_RCL_M_200401~(new)")

            # 테스트니까 한번만 실행되게
            # break

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
    # src_client = mongodb_connect(url='112.216.26.94:27017', id='messenger', pw='$triphos20!', initdb='MESSENGER')

    triphos = MongoDB_Default(
        mo_address="",
        mo_username="",
        mo_password="",
        mo_authsource="",
        mo_database="",
        mo_collection=None,
    )

    triphos.connectmongodb()
    #triphos.triphosdistinct2()
    triphos.triphosdistinct()
