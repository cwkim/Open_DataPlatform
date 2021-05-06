# -*- coding:utf-8 -*-

"""
    Title : RelDB_To_MySQL_Default
    Comment : ELEX를 통해 들어오는 전체 차량 수는 몇대일까
    Author : 최우정 / github : https://github.com/woojungchoi
"""

import pandas as pd
import pymysql
import csv
from datetime import timedelta, datetime
from pymongo import MongoClient
import sys

class MongoDBToMySQLDB_Default:
    def __init__(self, mo_address, mo_username, mo_password, mo_authsource, mo_database, mo_collection, my_host, my_port, my_username, my_password, my_database):
        self.mo_address = mo_address  # 주소
        self.mo_username = mo_username  # 사용자이름
        self.mo_password = mo_password  # 비밀번호
        self.mo_authsource = mo_authsource  # 권한
        self.mo_database = mo_database # DB이름
        self.mo_collection = mo_collection #Collection이름

        self.my_host = my_host  # 주소
        self.my_port = my_port  # 포트
        self.my_username = my_username  # 사용자이름
        self.my_password = my_password  # 비밀번호
        self.my_database = my_database  # DB이름

    def connectmongodb(self):
        """mongodb 연결"""

        self.mo_client = MongoClient(self.mo_address,
                                username=self.mo_username,
                                password=self.mo_password,
                                authSource=self.mo_authsource,
                                connect=False)
        self.mo_db = self.mo_client.get_database(self.mo_database)
        print('----------------------------------------------------')
        print('MongoDB Client Connected')
        print('address:', str(self.mo_address))
        print('databse:', self.mo_database)
        print('----------------------------------------------------\n')


    def connectmysqldb(self):
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
        print('address:', str(self.my_host),':',str(self.my_port))
        print('databse:', self.my_database)
        print('----------------------------------------------------\n')


    def mongodistinct(self):
        """"""

        print('----------------------------------------------------')
        print('mongodistinct')

        #collection list 가져오기
        collection_list = self.mo_db.collection_names()
        collection_list.sort()
        print(collection_list)

        car_list = []
        for col in collection_list:
            print('Collection Name:', col)
            mo_col = self.mo_db.get_collection(col)

            temp_list = mo_col.distinct("PHONE_NUM")
            car_list += temp_list
            #중복제거
            car_list = list(set(car_list))
            print('len:', len(temp_list))

        self.car_list = car_list

        print('all car_list loaded')
        print('len:',len(car_list))
        print('----------------------------------------------------')


if __name__ == '__main__':
    print('-----------------------------------------------------------------------------------------------------------')
    print('Start /','Time:', datetime.now())

    """데이터 이전 인스턴스 생성"""
    elex = MongoDBToMySQLDB_Default()

    """MongoDB(ReleaseDB) 연결"""
    elex.connectmongodb()

    """MySQL 연결"""
    elex.connectmysqldb()

    """차량목록생성"""
    elex.mongodistinct()
