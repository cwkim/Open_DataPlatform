# -*- coding:utf-8 -*-
import pandas as pd
import pymongo
import influxdb
import json
from pymongo import MongoClient
from DangerDriving import DangerDriving
import GetDataSet
    ''' 
    1. InfluxDB로 부터 데이터를 가져옴
    2. 위험운전(급가속 외 7개)을 계산하여 InfluxDB에 저장
    '''

if __name__ == '__main__':

    dataset = GetDataSet.GetDataSet(mo_address='',
                                mo_username='',
                                mo_password='',
                                mo_authsource='',
                                mo_database='',
                                mo_collection='',
                                in_host='', 
                                in_port='', 
                                in_username='', 
                                in_password='', 
                                in_database='')

    dataset.connectinfluxdf()
    
    date_list = pd.date_range() #날짜입력  
    carlist = [] # carid 입력

    danger = DangerDriving(in_host='',
                            in_port='', 
                            in_username='', 
                            in_password='', 
                            in_database='',
                            in_measurement='')

    for date in date_list:
        date = str(date)[:10]
        
        for carid in carlist:
            
            print("Date : ", date, "  ", "Carid : ", carid)
            query_ = f"SELECT * FROM {데이터셋 이름} WHERE time >= '{date}T00:00:00Z' and time <= '{date}T23:59:59Z' and car_id='{carid}'"
                  
            
            try:
                query_data = dataset.influx_client.query(query_)
                query_data = query_data[데이터셋 이름]
                query_data = query_data.rename_axis("RECORD_TIME").reset_index()
                
            except:
               continue
        
            if len(query_data) != 0:
                
                danger.DataframeToDict(query_data)
                driving.DataframeToDict(query_data)

                danger.getacceldecel()
                danger.savedangerlist()

                danger.get_turn_data(query_data)
                danger.savedangerlist()

                danger.get_uturn_data(query_data)
                danger.savedangerlist()

                danger.get_lane_switch_data(query_data)
                danger.savedangerlist() 

            else:
                continue
        