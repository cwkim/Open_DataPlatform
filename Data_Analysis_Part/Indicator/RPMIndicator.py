# -*- coding:utf-8 -*-

"""
    Title : 최근 차량운행데이터를 로드하여 RPM 관리 지표를 생성함


    Author : 최우정 / github : https://github.com/woojungchoi

"""

import pandas as pd
import influxdb
from datetime import timedelta, datetime
from pymongo import MongoClient

class GetRPMIndicator:
    def __init__(self, in_host, in_port, in_username, in_password, in_database, in_measurement):
        self.in_host = in_host  # 주소
        self.in_port = in_port  # 포트
        self.in_username = in_username  # 사용자이름
        self.in_password = in_password  # 비밀번호
        self.in_database = in_database  # DB이름
        self.in_measurement = in_measurement
        print('Object Created\n')


    def loaddata(self):
        """이전 저장한 데이터 수 만큼 skip후 데이터 쿼리"""

        print('Load Data')
        self.in_client = influxdb.DataFrameClient(host=self.in_host,
                                                  port=self.in_port,
                                                  username=self.in_username,
                                                  password=self.in_password,
                                                  database=self.in_database)
        print('InfluxB Connection complete\n')

        time = datetime.strftime(datetime.now() - timedelta(hours=10), "%Y-%m-%dT%H:%M:%SZ")
        query = "SELECT BATTERY_VOLTAGE, DRIVE_SPEED, car_id FROM MAIN2 WHERE time > '"+time+"' AND BATTERY_VOLTAGE!=0 AND RPM!=0"
        print('QUERY:\n\t',query)

        # 차량별 최신 위치 데이터 쿼리
        self.df =(self.in_client.query(query))['MAIN2']

        print('Dataframe loaded', '\nlen:', len(self.df), '\ncols:', self.df.columns)
        print(self.df.head())


    def getuniquelist(self):
        self.carlist = self.df['car_id'].unique()
        print('\n\nCarID list loaded', '\nlen:',len(self.carlist),'\n',self.carlist)


    def influxconnection(self):
        """데이터 저장을 위한 Influxdb client 연결"""

        self.in_client = influxdb.InfluxDBClient(host=self.in_host,
                                                port=self.in_port,
                                                username=self.in_username,
                                                password=self.in_password,
                                                database=self.in_database
                                                 )
        print('influxdb connection complete')
        print('address:', str(self.in_host)+':'+str(self.in_port))
        print('databse:', self.in_database)


    def __calcbatterymean(self, df):
        """배터리 평균값 계산"""

        battery_val = round(df['BATTERY_VOLTAGE'].mean()/10,3)
        print('배터리 지수:', battery_val, 'Volt')
        return battery_val

    def __saveinfluxdb(self, _carid, _batteryval, _savetime):
        """데이터 저장"""

        json_body = [
            {
                "measurement": "INDIC_BATTERY",
                "tags": {
                    "car_id": _carid
                },
                "time": _savetime,
                "fields": {
                    "CAR_ID": _carid,
                    "BATTERY_VOLTAGE": _batteryval,
                }
            }
        ]
        print(json_body)
        self.in_client.write_points(json_body)

    def mainindicator(self):
        """차량 for iteration 시작"""

        #데이터 저장을 위한 Influxdb 연결
        self.influxconnection()

        #데이터 저장 시간 계산
        savetime = datetime.now() - timedelta(hours=9)
        savetime = savetime.strftime('%Y-%m-%dT%H:%M:%SZ')

        print('차량 for iteration 시작')
        for car in range(len(self.carlist)):
            carid = self.carlist[car]
            print((car+1), ' /', len(self.carlist),'\ncar_id:', carid)

            #차량별 데이터프레임 생성
            temp_df = self.df[self.df['car_id']==carid]
            temp_df = temp_df[temp_df['DRIVE_SPEED']>0]

            #차량 운행시간 기준치 만족 확인
            timethres = 1200
            timedrive = len(temp_df)
            if timethres > timedrive: continue
            print('최근 1시간', round(timedrive / 60, 1), '분 운행')

            #주행중인 배터리볼트값 20분의 평균 구하기
            temp_df = temp_df.iloc[timedrive-timethres:]
            batteryval = self.__calcbatterymean(temp_df)

            #데이터 저장
            self.__saveinfluxdb(carid, batteryval, savetime)


if __name__ == '__main__':

    """데이터 이전 인스턴스 생성"""
    example = GetRPMIndicator(in_host=,
                                      in_port=,
                                      in_username=,
                                      in_password=,
                                      in_database=,
                                      in_measurement=
                                      )

    """InfluxDB(시계열)에서 최근 데이터 쿼리"""
    example.loaddata()

    """차량 리스트 선별"""
    example.getuniquelist()

    """지표 생성 및 저장 알고리즘 실행"""
    example.mainindicator()






