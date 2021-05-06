# -*- coding:utf-8 -*-

"""
    Title : Battery_Voltage_LSTM.py
    Comment : LSTM 모델을 통해 실제 배터리값을 실시간으로 예측하자!
    Author : 최우정 / github : https://github.com/woojungchoi
"""
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras import layers
from tensorflow.keras.layers import SimpleRNN, LSTM
from tensorflow.keras.optimizers import RMSprop, Adam
import tensorflow as tf
import pymysql
from datetime import timedelta, datetime
import sys
import pandas as pd
import seaborn as sns
import numpy as np
from matplotlib import pyplot as plt
from matplotlib import rcParams
import matplotlib as mpl
import time
from collections import OrderedDict
import requests
import warnings
from tensorflow.python.client import device_lib
from tensorflow.keras.models import model_from_json


class Prediction_Battery_Realtime:
    def __init__(self, my_host, my_port, my_username, my_password, my_database):
        self.my_host = my_host  # 주소
        self.my_port = my_port  # 포트
        self.my_username = my_username  # 사용자이름
        self.my_password = my_password  # 비밀번호
        self.my_database = my_database  # DB이름

        """LSTM을 위한 변수설정"""
        self.lookback = 600
        self.step = 2
        self.delay = 600
        self.batch_size = 100

        self.x_dataset = []
        self.y_dataset = []
        self.time_dataset = []
        self.dateset = []

        self.mean_bat = 28.16934476824763
        self.std_bat = 0.4206530933447228

        self.car_id = '01225789010'

        """Mysql 연결"""
        self.my_client = pymysql.connect(host=self.my_host,
                                                port=self.my_port,
                                                user=self.my_username,
                                                password=self.my_password,
                                                database=self.my_database
                                                 )
        print('----------------------------------------------------')
        print('mysql connection complete')
        print('----------------------------------------------------\n')

    def loadmodel(self):
        json_file = open("/home/wjchoi/github/keti_data_proc_develop/src/wjchoi/RMC_monitoring/LSTM/model/battery_daily_model.json", "r")
        loaded_model_json = json_file.read()
        json_file.close()
        self.loadedmodel = model_from_json(loaded_model_json)
        self.loadedmodel.load_weights("/home/wjchoi/github/keti_data_proc_develop/src/wjchoi/RMC_monitoring/LSTM/model/model_battery_daily_201105_163911.h5")
        print("Loaded model from disk")

        self.loadedmodel.compile(loss = 'mse', optimizer=RMSprop())


    def loaddata(self):
        """연결하는 부분"""

        query = """
            SELECT date, battery_mean, car_id, rnum FROM(
            	SELECT a.*,
            		(CASE @groupId WHEN a.car_id THEN @rownum:=@rownum+1 ELSE @rownum:=1 END) AS rnum,
            		(@groupId:=a.car_id)groupId
            	FROM DAILY_STATISTICS AS a
            	ORDER BY a.car_id, a.date desc 
            ) AS LOG
            WHERE rnum <=3
            ORDER BY car_id, rnum desc;
        """

        query = "SELECT date, battery_mean, car_id FROM DAILY_STATISTICS ORDER BY car_id desc, date;"
        print(query)

        curs = self.my_client.cursor()
        curs.execute(query)

        rows = curs.fetchall()
        i = 1
        self.df = list()
        for row in rows:
            self.df.append(row)

        self.df = pd.DataFrame(self.df)
        self.df.columns = ['date', 'battery', 'car_id']

        print(self.df)
        self.carlist = self.df['car_id'].unique()

        #print(self.carlist)



    def znormalization(self):
        """z-score normalization"""
        result = self.df
        print(result)

        self.df['battery'] = self.df['battery'] - self.mean_bat
        self.df['battery'] = self.df['battery'] / self.std_bat

        print(result)
        print('znormalization finished')


    def dateprocessing(self):
        """date 최근 4일치, 연속성 확인하기"""

        self.x_dataset = []
        self.id_dataset = []
        self.date_dataset = []

        dataset = self.df

        for car in self.carlist:

            temp_df = dataset[dataset['car_id'] == car]
            if len(temp_df) < 4: continue
            self.id_dataset.append(car)
            self.date_dataset.append(temp_df['date'].iloc[-1])
            temp_df = temp_df['battery'][-4:].to_numpy()


            self.x_dataset.append(temp_df)

        self.x_dataset = np.array(self.x_dataset)

        print(self.id_dataset)
        print(self.x_dataset)
        print(self.x_dataset.shape)
        print(self.date_dataset)
        print('\n\n')



    def prediction(self):

        self.x_dataset = np.array(self.x_dataset)
        print('x_dataset:', self.x_dataset)
        print('x_dataset_shape:', self.x_dataset.shape)
        self.x_dataset = self.x_dataset.reshape((self.x_dataset.shape[0], self.x_dataset.shape[1], 1))
        print('x_shape:', self.x_dataset.shape)

        prediction = self.loadedmodel.predict(self.x_dataset, batch_size=30, verbose=1)
        prediction = np.array(prediction)
        prediction = prediction.flatten()

        self.prediction = (prediction*self.std_bat)+ self.mean_bat
        print(self.prediction)


    def save(self):

        for i in range(len(self.prediction)):

            print()
            if (datetime.now().date() - self.date_dataset[i] > timedelta(days=4)): continue

            insert_sql = 'INSERT INTO BATTERY_PREDICTION(date, car_id, battery_pred)' \
                         'VALUE(\'{time}\', \'{car_id}\', {battery_pred})'.format(
                time=datetime.now().date(),
                car_id=self.id_dataset[i],
                battery_pred=self.prediction[i]
            )
            print(insert_sql)
            self.my_client.query(insert_sql)
            print('저장 완료')
        self.my_client.commit()


if __name__ == '__main__':

    """데이터 이전 인스턴스 생성"""
    batch = Prediction_Battery_Realtime(my_host=,
                                    my_port=,
                                    my_username=,
                                    my_password=,
                                    my_database=
                                )
    batch.loadmodel()
    batch.loaddata()
    batch.znormalization()
    batch.dateprocessing()
    batch.prediction()
    batch.save()