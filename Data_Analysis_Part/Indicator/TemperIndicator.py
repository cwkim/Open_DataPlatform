# -*- coding:utf-8 -*-

"""
    Title : 최근 차량운행데이터를 로드하여 센서 온도 관리 지표를 생성함


    Author : 최우정 / github : https://github.com/woojungchoi
    Last working date : 2020/04/20
"""

import pandas as pd
import numpy as np
import influxdb
from datetime import timedelta, datetime
from pymongo import MongoClient
import pymysql
import joblib
from sklearn.mixture import GaussianMixture

my_host='125.140.110.217',
my_port=3306,
my_user='dtc_admin',
my_password='admin12',
my_db='dtc_monit'

class GetTemperatureIndicator:
    def __init__(self, in_host, in_port, in_username, in_password, in_database, in_measurement, my_host, my_port, my_user, my_password, my_db):
        self.in_host = in_host  # 주소
        self.in_port = in_port  # 포트
        self.in_username = in_username  # 사용자이름
        self.in_password = in_password  # 비밀번호
        self.in_database = in_database  # DB이름
        self.in_measurement = in_measurement
        self.my_host = my_host
        self.my_port = my_port
        self.my_username = my_user
        self.my_password = my_password
        self.my_database = my_db
        print('Object Created\n')


    def loaddata(self):
        """이전 저장한 데이터 수 만큼 skip후 데이터 쿼리"""

        print('Load Data')
        self.in_client = influxdb.DataFrameClient(host=self.in_host,
                                                  port=self.in_port,
                                                  username=self.in_username,
                                                  password=self.in_password,
                                                  database=self.in_database)
        print('InfluxDB Connection complete\n')

        self.now = (datetime.now() - timedelta(hours=9))
        self.time = datetime.strftime(self.now - timedelta(minutes=7), "%Y-%m-%dT%H:%M:%SZ")
        self.now = datetime.strftime(self.now, '%Y-%m-%dT%H:%M:%SZ')
        #############################QUERY부터 다시 생성######################################
        query = "SELECT COOLANT_TEMPER, ENGINE_OIL_TEMPER, DRIVE_SPEED, car_id FROM MAIN2 WHERE time > '"+self.time+"' AND RPM!=0"
        print('QUERY:\n\t',query)

        # 차량별 최신 위치 데이터 쿼리
        self.df =(self.in_client.query(query))['MAIN2']

        print('Dataframe loaded', '\nlen:', len(self.df), '\ncols:', self.df.columns)
        print(self.df.head())


    def getuniquelist(self):
        self.carlist = self.df['car_id'].unique()
        print('\n\nCarID list loaded', '\nlen:',len(self.carlist),'\n',self.carlist)

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

    def mysqlconnection(self):
        """mysql 연결"""

        self.my_client = pymysql.connect(host=self.my_host,
                                         port=self.my_port,
                                         user=self.my_username,
                                         password=self.my_password,
                                         db=self.my_database,
                                         charset='utf8'
                                         )
        print('----------------------------------------------------')
        print('MySQL Client Connected')
        print('address:', str(self.my_host), ':', str(self.my_port))
        print('databse:', self.my_database)
        print('----------------------------------------------------\n')

    def gmmodelload(self):
        "가우시안혼합모델 - 파일 로드"
        self.gmmodel = joblib.load("/home/wjchoi/github/keti_data_proc_develop/src/wjchoi/RMC_monitoring/Sensor_Indicators/coolant_gm_2020_06_04T08#003A19#003A01Z_2020_06_11T08#003A19#003A01Z.pkl")

    def __aocdataframe(self, df):

        #시간 설정
        date = pd.date_range(self.time, self.now, freq = "s")
        data = pd.DataFrame(index= date, columns=['COOLANT_TEMPER', 'ENGINE_OIL_TEMPER', 'DRIVE_SPEED', 'car_id'])
        car_data = df
        data.loc[car_data.index] = car_data

        #AOC_VOLTAGE = data.BATTERY_VOLTAGE.sub(data.BATTERY_VOLTAGE.shift())
        AOC_COOLANT = data.COOLANT_TEMPER.sub(data.COOLANT_TEMPER.shift())
        #AOC_VOLTAGE.name = 'AOC_VOLTAGE'
        AOC_COOLANT.name = 'AOC_COOLANT'

        data = pd.concat([data, AOC_COOLANT], axis=1)

        data.dropna(axis=0)
        df = pd.concat([df, data])
        df = df.dropna(axis=0)

        return df

    def __gmoutlier(self, df):

        np_data = np.array(df[['COOLANT_TEMPER', 'AOC_COOLANT']])
        print('np_data:', np_data)
        densities = self.gmmodel.score_samples(np_data)
        density_threshold = np.percentile(densities, 0.01)
        anomalies = np_data[densities < density_threshold]

        if (len(anomalies) != 0):
            new_list = np.asarray([x for x in np_data if x not in anomalies])
            return pd.DataFrame(new_list)
        else:
            return pd.DataFrame(np_data)

    def __calccoolantvalue(self, df):
        """냉각수 온도 지표 계산"""

        coolant_val = round(df[0].mean()/10,3)
        print('냉각수 온도 지수:', coolant_val, '도')

        return coolant_val

    def __calcengineoilvalue(self, df):
        """엔진오일 온도 지표 계산"""

        #이상치만 들어오는 차량인지 확인해야 함
        #-40인지 0인지 암튼 통계값으로 검출해보자

        #coolant_val = round(df['COOLANT_TEMPER'].mean()/10,3)
        #print('냉각수 온도 지수:', coolant_val, '도')

        engineoil_val = round(df['ENGINE_OIL_TEMPER'].mean()/10, 3)
        print('엔진오일 온도 지수:', engineoil_val, '도')

        return engineoil_val

    def __savemysqldb(self, _carid, _coolantval, _engineoilval, _savetime, _coolant_degree, _engineoil_degree):
        """데이터 저장"""

        insert_coolantsql = 'INSERT INTO INDIC_COOLANT(time, coolantval, car_id, degree)' \
                     ' VALUE(\'{time}\', {coolantval}, \'{carid}\', {degree})'.format(
            time=_savetime,
            coolantval = _coolantval,
            carid=str(_carid),
            degree=_coolant_degree
        )
        print(insert_coolantsql)
        self.my_client.query(insert_coolantsql)

        insert_engineoilsql = 'INSERT INTO INDIC_ENGINEOIL(time, engineoilval, car_id, degree)' \
                     ' VALUE(\'{time}\', {engineoilval}, \'{carid}\', {degree})'.format(
            time=_savetime,
            engineoilval = _engineoilval,
            carid=str(_carid),
            degree=_engineoil_degree
        )
        print(insert_engineoilsql)
        self.my_client.query(insert_engineoilsql)
        print('저장 완료\n')

    def __saveinfluxdb(self, _carid, _coolantval, _engineoilval, _savetime, _coolant_degree, _engineoil_degree):
        """데이터 저장"""

        json_body = [
            {
                "measurement": "INDIC_TEMPER",
                "tags": {
                    "car_id": _carid
                },
                "time": _savetime,
                "fields": {
                    "CAR_ID": _carid,
                    "COOLANT_TEMPER": _coolantval,
                    "ENGINE_OIL_TEMPER": _engineoilval,
                    "COOLANT_DEGREE": _coolant_degree,
                    "ENGINE_OIL_DEGREE": _engineoil_degree
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

            #차량 운행시간 기준치 만족 확인
            timethres = 240
            timedrive = len(temp_df)
            if timethres > timedrive: continue
            print('최근 7시간', round(timedrive / 60, 1), '분 운행')

            # 이상치 제거를 위해 배터리값 증감량 데이터 생성
            coolant_df = self.__aocdataframe(temp_df)
            print(coolant_df)
            # 이상치 제거 알고리즘 실행
            coolant_df = self.__gmoutlier(coolant_df)
            print(coolant_df)

            #주행중인 온도값 지표 구하기
            coolantval = self.__calccoolantvalue(coolant_df)
            engineoilval = self.__calcengineoilvalue(temp_df)
            engineoilval = (engineoilval/2) + 20

            if(coolantval >= 80):coolant_degree = 3
            elif(coolantval >= 70): coolant_degree = 2
            elif(coolantval >= 60): coolant_degree = 1
            else: coolant_degree = 0

            if(engineoilval >= 130): engineoil_degree =3
            elif(engineoilval >=120): engineoil_degree =2
            elif(engineoilval >=110): engineoil_degree =1
            else: engineoil_degree = 0

            #데이터 저장
            #self.__saveinfluxdb(carid, coolantval, engineoilval, in_savetime, coolant_degree, engineoil_degree)
            self.__savemysqldb(carid, coolantval, engineoilval, my_savetime, coolant_degree, engineoil_degree)
        self.my_client.commit()

if __name__ == '__main__':

    """데이터 이전 인스턴스 생성"""
    example = GetTemperatureIndicator(in_host=,
                                      in_port=,
                                      in_username=,
                                      in_password=,
                                      in_database=,
                                      in_measurement=,
                                      my_host=,
                                      my_port=,
                                      my_user=,
                                      my_password=,
                                      my_db=
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

    """센터장님도 가시는건가"""