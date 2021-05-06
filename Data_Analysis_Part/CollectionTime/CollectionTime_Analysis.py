# -*- coding:utf-8 -*-

"""
    Title : 전체 차량 중 운행 중 항시 데이터를 저장하는 차량과 하루 중 지정된 시간대의 데이터만 저장하는 차량으로 구분합니다.

    Author : 최우정 / github : https://github.com/woojungchoi
"""


import time
import sys
import numpy as np
import pandas as pd
import influxdb
from pymongo import MongoClient
from datetime import datetime, timedelta
'''
print('-----------------------------------------------------------')
print('-----------------------------------------------------------\n\n')
'''


class CollectionTimeCheck:
    def __init__(self, mo_address, mo_username, mo_password, mo_authsource, mo_database, mo_collection, in_host,
                 in_port, in_username, in_password, in_database, in_measurement):
        self.mo_address = mo_address  # 주소
        self.mo_username = mo_username  # 사용자이름
        self.mo_password = mo_password  # 비밀번호
        self.mo_authsource = mo_authsource  # 권한
        self.mo_database = mo_database  # DB이름
        self.mo_collection = mo_collection  # Collection이름

        self.in_host = in_host  # 주소
        self.in_port = in_port  # 포트
        self.in_username = in_username  # 사용자이름
        self.in_password = in_password  # 비밀번호
        self.in_database = in_database  # DB이름
        self.in_measurement = in_measurement

        self.mon = mo_collection[-1]


    def mongodbconnection(self):
        """Mongodb client 연결"""

        self.mo_client = MongoClient(self.mo_address,
                                     username = self.mo_username,
                                     password = self.mo_password,
                                     authSource = self.mo_authsource,
                                     connect = False)
        self.mo_db = self.mo_client.get_database(self.mo_database)
        self.mo_coll = self.mo_db.get_collection(self.mo_collection)

        print('-----------------------------------------------------------')
        print('MongoDB Connection complete')
        print('DB:', self.mo_database, '\nCOLLECTION:', self.mo_collection)
        print('-----------------------------------------------------------\n\n')


    def influxconnection(self):
        """Influxdb client 연결"""

        self.in_client = influxdb.InfluxDBClient(host=self.in_host,
                                                port=self.in_port,
                                                username=self.in_username,
                                                password=self.in_password,
                                                database=self.in_database
                                                 )

        print('-----------------------------------------------------------')
        print('Influxdb Connection complete')
        print('ADDRESS:', str(self.in_host)+':'+str(self.in_port))
        print('DB:', self.in_database)
        print('-----------------------------------------------------------\n\n')


    def mogetcarlist(self):
        """차량 리스트 추출"""

        self.carlist = self.mo_coll.distinct('PHONE_NUM')

        print('-----------------------------------------------------------')
        print('CarID list loaded', '\nlen:',len(self.carlist),'\n',self.carlist)
        print('-----------------------------------------------------------\n\n')


    def __loadcardata(self, carid):

        cursor = self.mo_coll.find({'PHONE_NUM':carid},
                                    {"PHONE_NUM": 1, "RECORD_TIME": 1, "_id": 0})
        dictdata = pd.DataFrame(list(cursor)).to_dict()
        return dictdata

    def __calccollectiontime(self, dict):

        timelabel = ['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23']
        timetable = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        #print(dict)

        for val in dict['RECORD_TIME']:
            print(dict['RECORD_TIME'][val])
            time =int(str(dict['RECORD_TIME'][val])[11:13])
            print(time)
            timetable[time] +=1

        return timetable


    def __saveinfluxdb(self, _carid, timetable):
        """데이터 저장"""

        json_body = [
            {
                "measurement": self.in_measurement,
                "tags": {
                    "car_id": _carid
                },
                "time": '2020-0'+self.mon+'-01T00:00:00Z',
                "fields": {
                    "CAR_ID": _carid,
                    "0": timetable[0],
                    "1": timetable[1],
                    "2": timetable[2],
                    "3": timetable[3],
                    "4": timetable[4],
                    "5": timetable[5],
                    "6": timetable[6],
                    "7": timetable[7],
                    "8": timetable[8],
                    "9": timetable[9],
                    "10": timetable[10],
                    "11": timetable[11],
                    "12": timetable[12],
                    "13": timetable[13],
                    "14": timetable[14],
                    "15": timetable[15],
                    "16": timetable[16],
                    "17": timetable[17],
                    "18": timetable[18],
                    "19": timetable[19],
                    "20": timetable[20],
                    "21": timetable[21],
                    "22": timetable[22],
                    "23": timetable[23]
                }
            }
        ]
        print(json_body)
        self.in_client.write_points(json_body)


    def mainiteration(self):
        """차량별 iteration 실행"""

        print('-----------------------------------------------------------')
        print('차량별 데이터 수집 시간대 추출 시작')
        print('차량 수:', len(self.carlist))

        order = 0
        for car in self.carlist:

            print('\n', order, '/', len(self.carlist))
            order +=1
            if(order < 1399): continue
            print('car_id:', car)


            #차량 데이터 로드
            temp_df = self.__loadcardata(car)

            #수집 시간대 계산을 통한 타임테이블 생성
            time_table = self.__calccollectiontime(temp_df)
            print(time_table)

            #InfluxDB에 저장
            self.__saveinfluxdb(car, time_table)




if __name__ == '__main__':

    for i in range(9, 10):
        print(i, '월')
        """데이터 이전 인스턴스 생성"""
        check_Jan = CollectionTimeCheck()

        """MongoDB 접속"""
        check_Jan.mongodbconnection()

        """InfluxDB 접속"""
        check_Jan.influxconnection()

        """차량 리스트 추출"""
        check_Jan.mogetcarlist()

        """타임테이블 추출 테스트"""
        check_Jan.mainiteration()


        #8월 돌리기
        """
        'SELECT time, CAR_ID, "0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23" FROM COLLECTION_TIME WHERE time > '2020-08-01T00:00:00Z''        
        """