# -*- coding:utf-8 -*-

"""
    Title : 1일치의 데이터를 조회하여 통계값을 도출함

    Author : 최우정 / github : https://github.com/woojungchoi
"""

import pandas as pd
import influxdb
from datetime import timedelta, datetime
from pymongo import MongoClient
import pymysql

class GetSensorStatistics:
    def __init__(self, mo_address, mo_username, mo_password, mo_authsource, mo_database, mo_collection, in_host, in_port, in_username, in_password, in_database, in_measurement, my_host, my_port, my_user, my_password, my_db):
        self.mo_address = mo_address  # 주소
        self.mo_username = mo_username  # 사용자이름
        self.mo_password = mo_password  # 비밀번호
        self.mo_authsource = mo_authsource  # 권한
        self.mo_database = mo_database  # DB이름
        self.mo_collection = mo_collection  # Collection이름
        print(self.mo_collection)

        self.in_host = in_host  # 주소
        self.in_port = in_port  # 포트
        self.in_username = in_username  # 사용자이름
        self.in_password = in_password  # 비밀번호
        self.in_database = in_database  # DB이름
        self.in_measurement = in_measurement

        self.my_host = my_host  # 주소
        self.my_port = my_port  # 포트
        self.my_username = my_user  # 사용자이름
        self.my_password = my_password  # 비밀번호
        self.my_database = my_db  # DB이름
        print('Object Created\n')


    #def mongoconnection(self):
        mo_client = MongoClient(self.mo_address,
                                username=self.mo_username,
                                password=self.mo_password,
                                authSource=self.mo_authsource,
                                connect=False)
        mo_db = mo_client.get_database(self.mo_database)
        self.mo_coll = mo_db.get_collection(self.mo_collection)
        print('----------------------------------------------------')
        print('MongoDB Client Connected')
        print('address:', str(self.mo_address))
        print('databse:', self.mo_database)
        print('collection:', self.mo_collection)
        print('----------------------------------------------------\n')

    #def mysqlconnection(self):
        """mysql 연결"""

        self.my_client = pymysql.connect(host = self.my_host,
                                         port = self.my_port,
                                         user = self.my_username,
                                         password = self.my_password,
                                         db = self.my_database,
                                         charset= 'utf8'
                                         )
        print('----------------------------------------------------')
        print('MySQL Client Connected')
        print('address:'+str(self.my_host)+':'+str(self.my_port))
        print('databse:', self.my_database)
        print('----------------------------------------------------\n')

    #def influxconnection(self):
        """Influxdb 연결"""

        self.in_client = influxdb.InfluxDBClient(host=self.in_host,
                                                port=self.in_port,
                                                username=self.in_username,
                                                password=self.in_password,
                                                database=self.in_database
                                                 )
        print('----------------------------------------------------')
        print('influxdb connection complete')
        print('address:', str(self.in_host)+':'+str(self.in_port))
        print('databse:', self.in_database)
        print('----------------------------------------------------\n')

    def loaddata(self):
        """1일치 데이터 쿼리"""

        day=1

        query_lt = datetime.now()
        query_gte = (query_lt - timedelta(days=day))
        self.savedate = query_gte.strftime('%Y-%m-%d')
        # cursor = self.mo_coll.find({"RECORD_TIME": {"$gte": datetime(query_gte.year,query_gte.month, query_gte.day,0,0,0)
        #                                             , "$lt": datetime(query_lt.year, query_lt.month, query_lt.day,0,0,0)}},
        #                            {"PHONE_NUM": 1, "RECORD_TIME": 1, "BATTERY_VOLTAGE": 1, "COOLANT_TEMPER":1, "ENGINE_OIL_TEMPER":1, "_id": 0})
        cursor = self.mo_coll.find(
            {"RECORD_TIME": {"$regex":"^"+(datetime.now() - timedelta(days=day)).strftime('%Y-%m-%dT')}},
            {"PHONE_NUM": 1, "RECORD_TIME": 1, "BATTERY_VOLTAGE": 1, "COOLANT_TEMPER": 1, "ENGINE_OIL_TEMPER": 1,
             "_id": 0})

        self.df = pd.DataFrame(cursor)
        self.df = self.df.dropna()

        print('----------------------------------------------------')
        print('Data loaded')
        print('Query: find', query_gte.strftime('%Y-%m-%dT00:00:00Z'),'~',query_lt.strftime('%Y-%m-%dT00:00:00Z'))
        print('data len:', len(self.df))
        print(self.df.head(4))
        print('----------------------------------------------------\n')

    def getuniquelist(self):
        """조회된 데이터에서 차량 리스트를 추출함"""

        self.carlist = self.df['PHONE_NUM'].unique()
        print('----------------------------------------------------')
        print('\n\nCarID list loaded', '\nlen:',len(self.carlist))
        print('----------------------------------------------------\n')

    def __influxinsert(self, _list):

        json_body = [
            {
                "measurement": self.in_measurement,
                "tags": {
                    "car_id": _list[0]
                },
                "time": self.savedate,
                "fields": {
                    "CAR_ID": _list[0],
                    "BATTERY_MEAN": _list[1],
                    "BATTERY_MAX":_list[2],
                    "BATTERY_MIN":_list[3],
                    "COOLANT_MEAN":_list[4],
                    "COOLANT_MAX":_list[5],
                    "COOLANT_MIN":_list[6],
                    "ENGINEOIL_MEAN":_list[7],
                    "ENGINEOIL_MAX":_list[8],
                    "ENGINEOIL_MIN":_list[9],
                    "DATA_COUNT": int(_list[10]),
                }
            }
        ]
        print(json_body)
        self.in_client.write_points(json_body)

    def __mysqlinsert(self, list):
        """데이터 저장(MySQL)"""

        insert_sql = 'INSERT INTO DAILY_STATISTICS(date, car_id, battery_mean, battery_max, battery_min, coolant_mean, coolant_max, coolant_min, engineoil_mean, engineoil_max, engineoil_min, data_count)' \
                     'VALUE(\'{time}\', \'{car_id}\', {b_mean}, {b_max}, {b_min}, {c_mean}, {c_max}, {c_min}, {e_mean}, {e_max}, {e_min}, {data_count})'.format(
            time= self.savedate,
            car_id = list[0],
            b_mean = list[1],
            b_max = list[2],
            b_min = list[3],
            c_mean = list[4],
            c_max = list[5],
            c_min = list[6],
            e_mean = list[7],
            e_max = list[8],
            e_min = list[9],
            data_count = int(list[10])
        )
        print(insert_sql)
        self.my_client.query(insert_sql)
        print('저장 완료\n')

    def mainindicator(self):
        """차량 for iteration 시작"""

        print('차량 for iteration 시작')
        for car in range(len(self.carlist)):
            carid = self.carlist[car]
            print((car+1), ' /', len(self.carlist), '\ncar_id:', carid)

            #차량별 데이터프레임 생성
            temp_df = self.df[self.df['PHONE_NUM']==carid]

            statvalue = temp_df.describe()
            batteryvalue = temp_df[temp_df['BATTERY_VOLTAGE'] != 0]
            statbattery = batteryvalue['BATTERY_VOLTAGE'].describe()

            inputlist = [carid]
            inputlist.append(statbattery['mean']/10)
            inputlist.append(statbattery['max']/10)
            inputlist.append(statbattery['min']/10)
            inputlist.append(statvalue['COOLANT_TEMPER']['mean']/10)
            inputlist.append(statvalue['COOLANT_TEMPER']['max']/10)
            inputlist.append(statvalue['COOLANT_TEMPER']['min']/10)
            engineoil_mean = statvalue['ENGINE_OIL_TEMPER']['mean']/10
            engineoil_max = statvalue['ENGINE_OIL_TEMPER']['max']/10
            engineoil_min = statvalue['ENGINE_OIL_TEMPER']['min']/10

            engineoil_max = engineoil_max/2 + 20
            engineoil_mean = engineoil_mean/2 + 20
            engineoil_min = engineoil_min/2 + 20

            inputlist.append(engineoil_mean)
            inputlist.append(engineoil_max)
            inputlist.append(engineoil_min)
            inputlist.append(statvalue['ENGINE_OIL_TEMPER']['count'])
            print(inputlist)

            if str(inputlist[1])=='nan': continue

            self.__influxinsert(inputlist)
            self.__mysqlinsert(inputlist)
        self.my_client.commit()


if __name__ == '__main__':

    #1일 전
    day = 1
    """데이터 이전 인스턴스 생성"""
    batch = GetSensorStatistics(mo_address=,
                                    mo_username=,
                                    mo_password=,
                                    mo_authsource=,
                                    mo_database=,
                                    mo_collection='mdt'+(datetime.now() - timedelta(days=day)).strftime('%Y%m'),
                                    in_host=,
                                    in_port=,
                                    in_username=,
                                    in_password=,
                                    in_database=,
                                    in_measurement=,
                                    my_host = ,
                                    my_port = ,
                                    my_user = ,
                                    my_password = ,
                                    my_db =
                                )

    """날짜 쿼리 실행"""
    batch.loaddata()

    """차량 리스트 추출"""
    batch.getuniquelist()

    """차량별 결과 도출"""
    batch.mainindicator()