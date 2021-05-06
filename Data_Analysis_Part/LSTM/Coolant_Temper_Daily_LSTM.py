# -*- coding:utf-8 -*-

"""
    Title : Battery_Voltage_LSTM.py
    Comment : LSTM 모델을 통해 실제 배터리값을 실시간으로 예측하자!
    Author : 최우정 / github : https://github.com/woojungchoi
    Last working date : 2020/08/28
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


class Prediction_Temper_Realtime:
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

        self.min_mean_coolant = 0.7482930560695357
        self.min_std_coolant = 27.586013151422346

        self.mean_mean_coolant = 29.83308365248631
        self.mean_std_coolant = 14.998483544124033

        self.max_mean_coolant = 45.14922514661242
        self.max_std_coolant = 14.969121985753578

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
        min_json_file = open("/home/wjchoi/github/keti_data_proc_develop/src/wjchoi/RMC_monitoring/LSTM/model/model_coolant_min_daily.json", "r")
        mean_json_file = open("/home/wjchoi/github/keti_data_proc_develop/src/wjchoi/RMC_monitoring/LSTM/model/model_coolant_mean_daily.json", "r")
        max_json_file = open("/home/wjchoi/github/keti_data_proc_develop/src/wjchoi/RMC_monitoring/LSTM/model/model_coolant_max_daily.json", "r")

        self.min_loadedmodel = model_from_json(min_json_file.read())
        self.mean_loadedmodel = model_from_json(mean_json_file.read())
        self.max_loadedmodel = model_from_json(max_json_file.read())

        min_json_file.close()
        mean_json_file.close()
        max_json_file.close()

        self.min_loadedmodel.load_weights("/home/wjchoi/github/keti_data_proc_develop/src/wjchoi/RMC_monitoring/LSTM/model/model_coolant_min_daily.h5")
        self.mean_loadedmodel.load_weights("/home/wjchoi/github/keti_data_proc_develop/src/wjchoi/RMC_monitoring/LSTM/model/model_coolant_mean_daily.h5")
        self.max_loadedmodel.load_weights("/home/wjchoi/github/keti_data_proc_develop/src/wjchoi/RMC_monitoring/LSTM/model/model_coolant_max_daily.h5")

        print("Loaded model from disk")

        self.min_loadedmodel.compile(loss = 'mse', optimizer=RMSprop())
        self.mean_loadedmodel.compile(loss = 'mse', optimizer=RMSprop())
        self.max_loadedmodel.compile(loss = 'mse', optimizer=RMSprop())


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

        query = "SELECT date, coolant_min, coolant_mean, coolant_max, car_id FROM DAILY_STATISTICS ORDER BY car_id desc, date;"
        print(query)

        curs = self.my_client.cursor()
        curs.execute(query)

        rows = curs.fetchall()
        i = 1
        self.df = list()
        for row in rows:
            self.df.append(row)

        self.df = pd.DataFrame(self.df)
        self.df.columns = ['date', 'coolant_min', 'coolant_mean', 'coolant_max', 'car_id']

        print(self.df)
        self.carlist = self.df['car_id'].unique()



    def znormalization(self):
        """z-score normalization"""
        result = self.df
        print(result)

        self.df['coolant_min'] = self.df['coolant_min'] - self.min_mean_coolant
        self.df['coolant_min'] = self.df['coolant_min'] / self.min_std_coolant

        self.df['coolant_mmean'] = self.df['coolant_mean'] - self.mean_mean_coolant
        self.df['coolant_mean'] = self.df['coolant_mean'] / self.mean_std_coolant

        self.df['coolant_max'] = self.df['coolant_max'] - self.max_mean_coolant
        self.df['coolant_max'] = self.df['coolant_max'] / self.max_std_coolant

        print(result)
        print('znormalization finished')


    def dateprocessing(self):
        """date 최근 4일치, 연속성 확인하기"""

        self.min_x_dataset = []
        self.mean_x_dataset = []
        self.max_x_dataset = []

        self.id_dataset = []
        self.date_dataset = []

        dataset = self.df

        for car in self.carlist:

            temp_df = dataset[dataset['car_id'] == car]
            if len(temp_df) < 4: continue
            self.id_dataset.append(car)
            self.date_dataset.append(temp_df['date'].iloc[-1])
            min_temp_df = temp_df['coolant_min'][-4:].to_numpy()
            mean_temp_df = temp_df['coolant_mean'][-4:].to_numpy()
            max_temp_df = temp_df['coolant_max'][-4:].to_numpy()

            self.min_x_dataset.append(min_temp_df)
            self.mean_x_dataset.append(mean_temp_df)
            self.max_x_dataset.append(max_temp_df)

        self.min_x_dataset = np.array(self.min_x_dataset)
        self.mean_x_dataset = np.array(self.mean_x_dataset)
        self.max_x_dataset = np.array(self.max_x_dataset)

        print(self.id_dataset)
        print('min_x_dataset:', self.min_x_dataset)
        print('mean_x_dataset:', self.mean_x_dataset)
        print('max_x_dataset:', self.max_x_dataset)
        print(self.mean_x_dataset.shape)
        print(self.date_dataset)
        print('\n\n')


    def prediction(self):

        #self.x_dataset = np.array(self.x_dataset)

        self.min_x_dataset = np.array(self.min_x_dataset)
        self.mean_x_dataset = np.array(self.mean_x_dataset)
        self.max_x_dataset = np.array(self.max_x_dataset)

        # self.x_dataset = self.x_dataset.reshape((self.x_dataset.shape[0], self.x_dataset.shape[1], 1))
        # print('x_shape:', self.x_dataset.shape)

        self.min_x_dataset = self.min_x_dataset.reshape((self.min_x_dataset.shape[0], self.min_x_dataset.shape[1], 1))
        print('min_x_shape:', self.min_x_dataset.shape)
        self.mean_x_dataset = self.mean_x_dataset.reshape((self.mean_x_dataset.shape[0], self.mean_x_dataset.shape[1], 1))
        print('mean_x_shape:', self.mean_x_dataset.shape)
        self.max_x_dataset = self.max_x_dataset.reshape((self.max_x_dataset.shape[0], self.max_x_dataset.shape[1], 1))
        print('max_x_shape:', self.max_x_dataset.shape)

        # prediction = self.loadedmodel.predict(self.min_x_dataset, batch_size=30, verbose=1)
        # prediction = np.array(prediction)
        # prediction = prediction.flatten()

        min_prediction = self.min_loadedmodel.predict(self.min_x_dataset, batch_size=30, verbose=1)
        mean_prediction = self.mean_loadedmodel.predict(self.mean_x_dataset, batch_size=30, verbose=1)
        max_prediction = self.max_loadedmodel.predict(self.max_x_dataset, batch_size=30, verbose=1)

        self.min_prediction = (min_prediction*self.min_std_coolant)+ self.min_mean_coolant
        self.mean_prediction = (mean_prediction * self.mean_std_coolant) + self.mean_mean_coolant
        self.max_prediction = (max_prediction * self.max_std_coolant) + self.max_mean_coolant

        print(self.min_prediction)
        print(self.mean_prediction)
        print(self.max_prediction)


    def save(self):

        for i in range(len(self.min_prediction)):
            print()
            if (datetime.now().date() - self.date_dataset[i] > timedelta(days=4)): continue

            insert_sql = 'INSERT INTO TEMPER_PREDICTION(date, car_id, coolant_pred, min_coolant_pred, max_coolant_pred)' \
                         'VALUE(\'{time}\', \'{car_id}\', {coolant_pred}, {min_coolant_pred}, {max_coolant_pred})'.format(
                time=datetime.now().date(),
                car_id=self.id_dataset[i],
                coolant_pred=self.mean_prediction[i][0],
                min_coolant_pred=self.min_prediction[i][0],
                max_coolant_pred=self.max_prediction[i][0]
            )
            print(insert_sql)
            self.my_client.query(insert_sql)
            print('저장 완료')
        self.my_client.commit()


if __name__ == '__main__':

    """데이터 이전 인스턴스 생성"""
    batch = Prediction_Temper_Realtime(my_host=,
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