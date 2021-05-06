# -*- coding:utf-8 -*-

"""
    Title : 최근 차량운행데이터를 로드하여 배터리 관리 지표를 생성함


    Author : 최우정 / github : https://github.com/woojungchoi
"""

import pandas as pd
import influxdb
from datetime import timedelta, datetime
import pymysql
import joblib
import numpy as np
from pymongo import MongoClient
from sklearn.mixture import GaussianMixture

class GetBatteryIndicator:
    def __init__(self, in_host, in_port, in_username, in_password, in_database, in_measurement, my_host, my_port, my_user, my_password, my_db):
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

    def mysqlconnection(self):
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

    def gmmodelload(self):
        "가우시안혼합모델 - 파일 로드"

        self.gmmodel = joblib.load("/home/wjchoi/github/keti_data_proc_develop/src/wjchoi/RMC_monitoring/Sensor_Indicators/battery_gm_2020-06-01T07_2020-06-08T07.pkl")

    def loaddata(self):
        """이전 저장한 데이터 수 만큼 skip후 데이터 쿼리"""

        print('Load Data')
        self.in_client = influxdb.DataFrameClient(host=self.in_host,
                                                  port=self.in_port,
                                                  username=self.in_username,
                                                  password=self.in_password,
                                                  database=self.in_database
                                                  )
        print('InfluxDB Connection complete\n')

        self.now = (datetime.now() - timedelta(hours=9))
        self.time = datetime.strftime(self.now - timedelta(minutes=7), "%Y-%m-%dT%H:%M:%SZ")
        self.now = datetime.strftime(self.now, '%Y-%m-%dT%H:%M:%SZ')
        query = "SELECT BATTERY_VOLTAGE, DRIVE_SPEED, car_id FROM MAIN2 WHERE time > '"+self.time+"' AND BATTERY_VOLTAGE!=0 AND RPM!=0"
        print('QUERY:\n\t',query)

        # 차량별 최신 위치 데이터 쿼리
        self.df =(self.in_client.query(query))['MAIN2']

        print('Dataframe loaded', '\nlen:', len(self.df), '\ncols:', self.df.columns)
        print(self.df.head())

    def getuniquelist(self):
        self.carlist = self.df['car_id'].unique()
        print('\n\nCarID list loaded', '\nlen:',len(self.carlist),'\n',self.carlist)

    def __aocdataframe(self, df):

        #시간 설정
        date = pd.date_range(self.time, self.now, freq = "s")
        data = pd.DataFrame(index= date, columns=['BATTERY_VOLTAGE', 'DRIVE_SPEED', 'car_id'])
        car_data = df
        data.loc[car_data.index] = car_data

        AOC_VOLTAGE = data.BATTERY_VOLTAGE.sub(data.BATTERY_VOLTAGE.shift())
        #AOC_SPEED = data.DRIVE_SPEED.sub(data.DRIVE_SPEED.shift())
        AOC_VOLTAGE.name = 'AOC_VOLTAGE'
        #AOC_SPEED.name = 'AOC_SPEED'

        #data = pd.concat([data, AOC_VOLTAGE, AOC_SPEED], axis=1)
        data = pd.concat([data, AOC_VOLTAGE], axis=1)
        print('data:', data)
        data.dropna(axis=0)
        df = pd.concat([df, data])
        df = df.dropna(axis=0)

        return df

    def __gmoutlier(self, df):

        np_data = np.array(df[['BATTERY_VOLTAGE', 'AOC_VOLTAGE']])
        densities = self.gmmodel.score_samples(np_data)
        density_threshold = np.percentile(densities, 0.01)
        anomalies = np_data[densities < density_threshold]

        if(len(anomalies) != 0):
            new_list = np.asarray([x for x in np_data if x not in anomalies])
            return pd.DataFrame(new_list)
        else:
            return pd.DataFrame(np_data)

    def __calcbatterymean(self, df):
        """배터리 평균값 계산"""

        battery_val = round(df[0].mean()/10,3)
        print('배터리 지수:', battery_val, 'Volt')
        return battery_val

    def __savemysqldb(self, _carid, _batteryval, _savetime, _degree):
        """데이터 저장"""

        insert_sql = 'INSERT INTO INDIC_BATTERY(time, batteryval, car_id, degree)' \
                     ' VALUE(\'{time}\', {batteryval}, \'{carid}\', {degree})'.format(
            time=_savetime,
            batteryval=_batteryval,
            carid=str(_carid),
            degree = int(_degree)
        )
        print(insert_sql)
        self.my_client.query(insert_sql)
        print('저장 완료\n')

    def __saveinfluxdb(self, _carid, _batteryval, _savetime, _degree):
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
                    "DEGREE": _degree
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
        in_savetime = savetime.strftime('%Y-%m-%dT%H:%M:%SZ')
        my_savetime = savetime.strftime('%Y-%m-%d %H:%M:%S')

        print('차량 for iteration 시작')
        for car in range(len(self.carlist)):
            carid = self.carlist[car]
            print((car+1), ' /', len(self.carlist),'\ncar_id:', carid)

            #차량별 데이터프레임 생성
            temp_df = self.df[self.df['car_id']==carid]
            temp_df = temp_df[temp_df['DRIVE_SPEED']>0]

            #차량 운행시간 기준치 만족 확인
            timethres = 240
            timedrive = len(temp_df)
            if timethres > timedrive: continue
            print('최근 7분 중 ', round(timedrive / 60, 1), '분 운행')

            #이상치 제거를 위해 배터리값 증감량 데이터 생성
            temp_df = self.__aocdataframe(temp_df)
            print(temp_df)
            #이상치 제거 알고리즘 실행
            temp_df = self.__gmoutlier(temp_df)
            print(temp_df)

            #주행중인 배터리볼트값 평균 구하기
            batteryval = self.__calcbatterymean(temp_df)
            print(batteryval)

            #위험정도 나타내기
            if(batteryval < 27): degree = 3
            elif(batteryval < 27.5): degree = 2
            elif(batteryval < 28): degree = 1
            else: degree = 0

            if batteryval > 10 and batteryval < 17: continue

            #데이터 저장
            #self.__saveinfluxdb(carid, batteryval, in_savetime, degree)
            self.__savemysqldb(carid, batteryval, my_savetime, degree)
        self.my_client.commit()

if __name__ == '__main__':

    """데이터 이전 인스턴스 생성"""
    example = GetBatteryIndicator(in_host=,
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

    """MySQL(시계열)에서"""
    example.mysqlconnection()

    """이상치탐지모델 로드"""
    example.gmmodelload()

    """InfluxDB(시계열)에서 최근 데이터 쿼리"""
    example.loaddata()

    """차량 리스트 선별"""
    example.getuniquelist()

    """지표 생성 및 저장 알고리즘 실행"""
    example.mainindicator()